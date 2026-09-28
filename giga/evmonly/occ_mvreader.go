package evmonly

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
)

// mvEstimateAbort is raised (as a panic inside the StateDB callbacks, then
// recovered by executeIncarnation) when an incarnation needs a location that a
// lower transaction is about to rewrite.
type mvEstimateAbort struct {
	txIdx         int
	blockingTxIdx int
	key           stateAccessKey
}

func (a *mvEstimateAbort) Error() string {
	return fmt.Sprintf("tx %d read an estimate of tx %d at %s %s", a.txIdx, a.blockingTxIdx, a.key.kind, a.key.address)
}

// mvVersionedReader is the StateReader one incarnation runs against. It
// resolves every source-level read through multi-version memory, memoises the
// answer so the incarnation sees one consistent value per key, and records the
// semantic reads nativeStateDB reports so validation covers exactly the keys the
// transaction depended on.
type mvVersionedReader struct {
	mem         *mvMemory
	txIdx       int
	incarnation int
	// resolved holds every source-level resolution of this incarnation.
	resolved map[stateAccessKey]*mvRead
	// needed holds every key nativeStateDB marked as read or written.
	needed map[stateAccessKey]struct{}
	// pendingEstimate holds keys that resolved to an estimate before the
	// transaction needed them. Needing one of them aborts.
	pendingEstimate map[stateAccessKey]int
	foldBuf         []txVersion
}

var _ StateReader = (*mvVersionedReader)(nil)
var _ readObserver = (*mvVersionedReader)(nil)

func newMVVersionedReader() *mvVersionedReader {
	return &mvVersionedReader{
		resolved:        map[stateAccessKey]*mvRead{},
		needed:          map[stateAccessKey]struct{}{},
		pendingEstimate: map[stateAccessKey]int{},
	}
}

func (r *mvVersionedReader) reset(mem *mvMemory, txIdx, incarnation int) {
	r.mem = mem
	r.txIdx = txIdx
	r.incarnation = incarnation
	clear(r.resolved)
	clear(r.needed)
	clear(r.pendingEstimate)
	r.foldBuf = r.foldBuf[:0]
}

func (r *mvVersionedReader) abort(key stateAccessKey, blocking int) {
	panic(&mvEstimateAbort{txIdx: r.txIdx, blockingTxIdx: blocking, key: key})
}

// need marks a key as semantically accessed and aborts if it had resolved to
// an estimate.
func (r *mvVersionedReader) need(key stateAccessKey) {
	r.needed[key] = struct{}{}
	if blocking, ok := r.pendingEstimate[key]; ok {
		r.abort(key, blocking)
	}
}

func (r *mvVersionedReader) observeRead(key stateAccessKey) {
	r.observe(key)
}

func (r *mvVersionedReader) observeWrite(key stateAccessKey) {
	r.observe(key)
}

func (r *mvVersionedReader) observe(key stateAccessKey) {
	if key.kind == stateAccessAccount {
		// Existence and emptiness are derived from balance, nonce and code.
		r.need(stateAccessKey{kind: stateAccessBalance, address: key.address})
		r.need(stateAccessKey{kind: stateAccessNonce, address: key.address})
		r.need(stateAccessKey{kind: stateAccessCode, address: key.address})
		return
	}
	r.need(key)
}

// onEstimate handles a resolution that hit an estimate: abort now if the
// transaction already needs the key, otherwise remember it and let the caller
// return a placeholder that can never reach the output unneeded.
func (r *mvVersionedReader) onEstimate(key stateAccessKey, blocking int) {
	if _, needed := r.needed[key]; needed {
		r.abort(key, blocking)
	}
	r.pendingEstimate[key] = blocking
}

func (r *mvVersionedReader) GetBalance(addr common.Address) *big.Int {
	key := stateAccessKey{kind: stateAccessBalance, address: addr}
	if rd, ok := r.resolved[key]; ok {
		return rd.u256.ToBig()
	}
	val, res := r.mem.resolveBalance(addr, r.txIdx, r.foldBuf[:0])
	if res.blocking >= 0 {
		r.onEstimate(key, res.blocking)
		return new(big.Int)
	}
	if res.overflow {
		// Surface the same error loadAccount raises for an out-of-range balance
		// by handing it a value uint256FromBig rejects.
		return new(big.Int).Lsh(big.NewInt(1), 256)
	}
	rd := &mvRead{key: key, origin: res.origin, u256: val}
	if len(res.fold) > 0 {
		rd.fold = append([]txVersion(nil), res.fold...)
	}
	r.resolved[key] = rd
	return val.ToBig()
}

func (r *mvVersionedReader) GetNonce(addr common.Address) uint64 {
	key := stateAccessKey{kind: stateAccessNonce, address: addr}
	if rd, ok := r.resolved[key]; ok {
		return rd.nonce
	}
	val, res := r.mem.resolveNonce(addr, r.txIdx)
	if res.blocking >= 0 {
		r.onEstimate(key, res.blocking)
		return 0
	}
	r.resolved[key] = &mvRead{key: key, origin: res.origin, nonce: val}
	return val
}

func (r *mvVersionedReader) GetCode(addr common.Address) []byte {
	key := stateAccessKey{kind: stateAccessCode, address: addr}
	if rd, ok := r.resolved[key]; ok {
		return rd.code
	}
	val, res := r.mem.resolveCode(addr, r.txIdx)
	if res.blocking >= 0 {
		r.onEstimate(key, res.blocking)
		return nil
	}
	r.resolved[key] = &mvRead{key: key, origin: res.origin, code: val}
	return val
}

func (r *mvVersionedReader) GetState(addr common.Address, slot common.Hash) common.Hash {
	key := stateAccessKey{kind: stateAccessStorage, address: addr, slot: slot}
	if rd, ok := r.resolved[key]; ok {
		return rd.u256.Bytes32()
	}
	val, res := r.mem.resolveStorage(addr, slot, r.txIdx)
	if res.blocking >= 0 {
		r.onEstimate(key, res.blocking)
		return common.Hash{}
	}
	rd := &mvRead{key: key, origin: res.origin}
	rd.u256.SetBytes32(val[:])
	r.resolved[key] = rd
	return val
}

// collect returns the reads validation must check: every key the transaction
// needed that was resolved from multi-version memory or the snapshot.
func (r *mvVersionedReader) collect() []mvRead {
	reads := make([]mvRead, 0, len(r.needed))
	for key := range r.needed {
		if rd, ok := r.resolved[key]; ok {
			reads = append(reads, *rd)
		}
	}
	return reads
}

func (e *Executor) acquireMVReader(mem *mvMemory, txIdx, incarnation int) *mvVersionedReader {
	var reader *mvVersionedReader
	if v := e.mvReaderPool.Get(); v != nil {
		reader = v.(*mvVersionedReader)
	} else {
		reader = newMVVersionedReader()
	}
	reader.reset(mem, txIdx, incarnation)
	return reader
}

func (e *Executor) releaseMVReader(reader *mvVersionedReader) {
	if reader == nil {
		return
	}
	reader.reset(nil, 0, 0)
	e.mvReaderPool.Put(reader)
}

func (e *Executor) acquireMVMemory(snapshot StateReader, txCount int) *mvMemory {
	var mem *mvMemory
	if v := e.mvMemoryPool.Get(); v != nil {
		mem = v.(*mvMemory)
	} else {
		mem = newMVMemory()
	}
	mem.reset(snapshot, txCount)
	return mem
}

func (e *Executor) releaseMVMemory(mem *mvMemory) {
	if mem == nil {
		return
	}
	mem.reset(nil, 0)
	e.mvMemoryPool.Put(mem)
}

// executeIncarnation runs one incarnation of one transaction against
// multi-version memory. A read of an estimate aborts execution and is returned
// as an *mvEstimateAbort error; any other panic propagates.
func (e *Executor) executeIncarnation(
	ctx context.Context,
	mem *mvMemory,
	req PreparedBlock,
	txIdx int,
	txIdxUint uint,
	incarnation int,
	chainConfig *params.ChainConfig,
	blockCtx vm.BlockContext,
	baseFee *big.Int,
	gasLimit uint64,
) (result occTxExecution, err error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return occTxExecution{}, ctxErr
	}
	defer func() {
		if rec := recover(); rec != nil {
			abort, ok := rec.(*mvEstimateAbort)
			if !ok {
				panic(rec)
			}
			result, err = occTxExecution{incarnation: incarnation}, abort
		}
	}()
	reader := e.acquireMVReader(mem, txIdx, incarnation)
	defer e.releaseMVReader(reader)
	p := req.Txs[txIdx]
	stateDB := e.acquireStateDB(reader)
	defer e.releaseStateDB(stateDB)
	stateDB.enableAccessTracking()
	evm := vm.NewEVM(blockCtx, stateDB, chainConfig, vm.Config{}, nil)
	stateDB.SetEVM(evm)
	gasPool := new(core.GasPool).AddGas(gasLimit)
	txResult, receipt, execErr := e.executeTx(
		evm,
		stateDB,
		gasPool,
		req.Context,
		p,
		txIdx,
		txIdxUint,
		baseFee,
	)
	_, writeSet := stateDB.takeAccessSets()
	result = occTxExecution{
		txResult:                 txResult,
		receipt:                  receipt,
		reads:                    reader.collect(),
		writeSet:                 writeSet,
		gasUsed:                  txResult.GasUsed,
		gasLimit:                 p.Tx.Gas(),
		commutativeBalanceDeltas: stateDB.commutativeBalanceDeltasBig(),
		incarnation:              incarnation,
	}
	if execErr != nil {
		result.err = fmt.Errorf("execute tx %d %s: %w", txIdx, p.Tx.Hash(), execErr)
		return result, nil
	}
	stateDB.ChangeSetInto(&result.changeSet)
	return result, nil
}
