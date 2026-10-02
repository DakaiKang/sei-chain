package scenarios

import (
	"context"
	"fmt"
	"math/big"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/sei-protocol/sei-chain/giga/evmonly"
)

// Arms of the divergent-rw workload. Both do one load and two stores over the
// same calldata; they differ only in whether the stored slot can be known
// before the transaction runs.
const (
	DivergentOpDivergent = "divergent"
	DivergentOpControl   = "control"
)

// Calldata is four words, identical in both arms so they cost the same to
// send: the hot slot, the target space, the target base, and the slot the
// control arm stores at.
const divergentCalldataWords = 4

var (
	// Loads the hot slot's counter x, stores x at base+(x mod space), then
	// writes x+1 back to the hot slot. The stored slot is a function of state
	// another transaction is concurrently advancing, so an execution that read
	// a stale counter stores the wrong slot and a re-execution stores a
	// different one: the write set itself moves.
	//
	// PUSH1 0, CALLDATALOAD, DUP1, SLOAD, DUP1, PUSH1 32, CALLDATALOAD, SWAP1,
	// MOD, PUSH1 64, CALLDATALOAD, ADD, DUP2, SWAP1, SSTORE, PUSH1 1, ADD,
	// SWAP1, SSTORE, STOP
	divergentRuntimeCode = common.FromHex("0x600035805480602035900660403501819055600101905500")
	// The matched control: the same load and two stores, but the stored slot
	// comes from calldata, so it is known before execution and survives a
	// re-execution unchanged.
	//
	// PUSH1 0, CALLDATALOAD, DUP1, SLOAD, PUSH1 96, CALLDATALOAD, DUP2, SWAP1,
	// SSTORE, PUSH1 1, ADD, SWAP1, SSTORE, STOP
	divergentControlRuntimeCode = common.FromHex("0x6000358054606035819055600101905500")
)

// DivergentRWWorkload makes every transaction read one of Config.DivergentFanout
// hot counter slots, write a slot derived from the counter, and advance the
// counter. Transactions sharing a hot slot form a dependency chain of
// TxsPerBlock/DivergentFanout, and under the divergent arm each link's write
// set is only settled once it has read its predecessor's value.
type DivergentRWWorkload struct {
	cfg           Config
	state         State
	signer        ethtypes.Signer
	accountCursor atomic.Uint64
	targetBase    *big.Int
	// perChainPerBlock is how far each block advances one chain's counter, so
	// the control arm's target advances across blocks exactly as the divergent
	// arm's does.
	perChainPerBlock uint64
}

// divergentTargetBase keeps derived slots clear of the hot counters, which
// occupy the low slots.
var divergentTargetBase = new(big.Int).Lsh(big.NewInt(1), 64)

func NewDivergentRWWorkload(cfg Config, state State) (*DivergentRWWorkload, error) {
	if cfg.DivergentFanout <= 0 {
		return nil, fmt.Errorf("divergent-fanout must be positive")
	}
	if cfg.DivergentTargetSpace <= 0 {
		return nil, fmt.Errorf("divergent-target-space must be positive")
	}
	code, err := divergentRuntimeCodeFor(cfg.DivergentOp)
	if err != nil {
		return nil, err
	}
	state.SetCode(cfg.DivergentContract, code)
	// Seed the counters spread across the target space so each chain derives a
	// different slot, and so the first read resolves to a written value rather
	// than the implicit zero. Chains that collided on one target would add a
	// contention the experiment is not trying to measure.
	for i := 0; i < cfg.DivergentFanout; i++ {
		state.SetState(cfg.DivergentContract, divergentHotSlot(i), common.BigToHash(divergentChainSeed(cfg, i)))
	}
	return &DivergentRWWorkload{
		cfg:        cfg,
		state:      state,
		signer:     ethtypes.LatestSignerForChainID(cfg.ChainID),
		targetBase: divergentTargetBase,
		perChainPerBlock: uint64FromNonNegativeInt(
			(cfg.TxsPerBlock + cfg.DivergentFanout - 1) / cfg.DivergentFanout,
		),
	}, nil
}

func divergentRuntimeCodeFor(op string) ([]byte, error) {
	switch op {
	case DivergentOpDivergent:
		return divergentRuntimeCode, nil
	case DivergentOpControl:
		return divergentControlRuntimeCode, nil
	default:
		return nil, fmt.Errorf("unsupported divergent operation %q", op)
	}
}

// divergentChainSeed spaces the chains' starting counters evenly over the
// target space.
func divergentChainSeed(cfg Config, chain int) *big.Int {
	stride := cfg.DivergentTargetSpace / cfg.DivergentFanout
	if stride < 1 {
		stride = 1
	}
	return new(big.Int).SetUint64(uint64FromNonNegativeInt(chain * stride))
}

func divergentHotSlot(index int) common.Hash {
	return common.BigToHash(new(big.Int).SetUint64(uint64FromNonNegativeInt(index)))
}

func (w *DivergentRWWorkload) BuildBlock(ctx context.Context, number uint64) (evmonly.BlockRequest, error) {
	txs := make([][]byte, w.cfg.TxsPerBlock)
	for i := 0; i < w.cfg.TxsPerBlock; i++ {
		select {
		case <-ctx.Done():
			return evmonly.BlockRequest{}, ctx.Err()
		default:
		}
		accountIndex := w.accountCursor.Add(1)
		raw, sender, err := w.buildCallTx(accountIndex, i, number)
		if err != nil {
			return evmonly.BlockRequest{}, err
		}
		w.state.SetBalance(sender, w.cfg.SenderBalance)
		txs[i] = raw
	}
	return evmonly.BlockRequest{
		Context: BlockContext(w.cfg, number),
		Txs:     txs,
	}, nil
}

// HotSlot returns the counter slot the transaction at txIndex reads and
// advances.
func (w *DivergentRWWorkload) HotSlot(txIndex int) common.Hash {
	return divergentHotSlot(txIndex % w.cfg.DivergentFanout)
}

func (w *DivergentRWWorkload) buildCallTx(accountIndex uint64, txIndex int, blockNumber uint64) ([]byte, common.Address, error) {
	key, err := DeterministicPrivateKey(accountIndex)
	if err != nil {
		return nil, common.Address{}, err
	}
	sender := crypto.PubkeyToAddress(key.PublicKey)
	tx := ethtypes.NewTx(&ethtypes.LegacyTx{
		Nonce:    0,
		GasPrice: new(big.Int).Set(w.cfg.GasPrice),
		Gas:      w.cfg.TxGasLimit,
		To:       &w.cfg.DivergentContract,
		Value:    new(big.Int),
		Data:     w.calldata(txIndex, blockNumber),
	})
	signed, err := ethtypes.SignTx(tx, w.signer, key)
	if err != nil {
		return nil, common.Address{}, err
	}
	raw, err := signed.MarshalBinary()
	if err != nil {
		return nil, common.Address{}, err
	}
	return raw, sender, nil
}

// calldata lays out the hot slot, the target space, the target base, and the
// control arm's target.
//
// The control target is this transaction's own chain seed advanced by its
// position along that chain, which is what the divergent arm derives once it
// has read the counter, up to the constant each earlier block added. Both arms
// therefore spread over the target space the same way and write the same
// number of distinct slots per block; only the divergent arm has to execute to
// learn which slot that is.
func (w *DivergentRWWorkload) calldata(txIndex int, blockNumber uint64) []byte {
	space := uint64FromNonNegativeInt(w.cfg.DivergentTargetSpace)
	seed := divergentChainSeed(w.cfg, txIndex%w.cfg.DivergentFanout)
	position := blockNumber*w.perChainPerBlock + uint64FromNonNegativeInt(txIndex/w.cfg.DivergentFanout) + 1
	controlTarget := new(big.Int).SetUint64((seed.Uint64() + position) % space)
	controlTarget.Add(controlTarget, w.targetBase)

	data := make([]byte, 32*divergentCalldataWords)
	copy(data[:32], w.HotSlot(txIndex).Bytes())
	copy(data[32:64], common.BigToHash(new(big.Int).SetUint64(space)).Bytes())
	copy(data[64:96], common.BigToHash(w.targetBase).Bytes())
	copy(data[96:128], common.BigToHash(controlTarget).Bytes())
	return data
}
