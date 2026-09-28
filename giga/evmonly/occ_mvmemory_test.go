package evmonly

import (
	"errors"
	"math/big"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

var (
	mvTestAddr  = common.HexToAddress("0x00000000000000000000000000000000000000aa")
	mvTestAddr2 = common.HexToAddress("0x00000000000000000000000000000000000000bb")
	mvTestSlot  = common.BytesToHash([]byte{1})
	mvTestSlot2 = common.BytesToHash([]byte{2})
)

func mvTestMemory(t *testing.T, txCount int, snapshot *MemoryState) *mvMemory {
	t.Helper()
	m := newMVMemory()
	m.reset(snapshot, txCount)
	return m
}

func mvDeltaExec(addr common.Address, post, delta int64) *occTxExecution {
	return &occTxExecution{
		changeSet:                StateChangeSet{Balances: []BalanceChange{{Address: addr, Balance: big.NewInt(post)}}},
		commutativeBalanceDeltas: map[common.Address]*big.Int{addr: big.NewInt(delta)},
		writeSet:                 map[stateAccessKey]struct{}{},
	}
}

func mvBalanceExec(addr common.Address, post int64) *occTxExecution {
	return &occTxExecution{
		changeSet: StateChangeSet{Balances: []BalanceChange{{Address: addr, Balance: big.NewInt(post)}}},
		writeSet:  map[stateAccessKey]struct{}{{kind: stateAccessBalance, address: addr}: {}},
	}
}

func mvStorageExec(addr common.Address, slots map[common.Hash]common.Hash, clear bool) *occTxExecution {
	exec := &occTxExecution{writeSet: map[stateAccessKey]struct{}{}}
	if clear {
		exec.changeSet.StorageClears = []common.Address{addr}
	}
	for k, v := range slots {
		exec.changeSet.Storage = append(exec.changeSet.Storage, StorageChange{Address: addr, Key: k, Value: v, Delete: v == (common.Hash{})})
		exec.writeSet[stateAccessKey{kind: stateAccessStorage, address: addr, slot: k}] = struct{}{}
	}
	return exec
}

func TestMVMemoryBalanceFold(t *testing.T) {
	snap := NewMemoryState()
	snap.SetBalance(mvTestAddr, big.NewInt(100))
	m := mvTestMemory(t, 6, snap)

	require.True(t, m.record(1, 0, mvDeltaExec(mvTestAddr, 105, 5)))
	require.True(t, m.record(2, 0, mvDeltaExec(mvTestAddr, 112, 7)))
	require.True(t, m.record(3, 0, mvBalanceExec(mvTestAddr, 50)))
	require.True(t, m.record(4, 0, mvDeltaExec(mvTestAddr, 51, 1)))

	val, res := m.resolveBalance(mvTestAddr, 3, nil)
	require.Equal(t, -1, res.blocking)
	require.Equal(t, uint64(112), val.Uint64())
	require.Equal(t, originSnapshot, res.origin.kind)
	require.Equal(t, []txVersion{{txIdx: 2}, {txIdx: 1}}, res.fold)

	val, res = m.resolveBalance(mvTestAddr, 5, nil)
	require.Equal(t, uint64(51), val.Uint64())
	require.Equal(t, readOrigin{kind: originVersion, version: txVersion{txIdx: 3}}, res.origin)
	require.Equal(t, []txVersion{{txIdx: 4}}, res.fold)

	val, res = m.resolveBalance(mvTestAddr, 1, nil)
	require.Equal(t, uint64(100), val.Uint64())
	require.Equal(t, originSnapshot, res.origin.kind)
	require.Empty(t, res.fold)
}

func TestMVMemoryEstimateInFold(t *testing.T) {
	snap := NewMemoryState()
	snap.SetBalance(mvTestAddr, big.NewInt(100))
	m := mvTestMemory(t, 4, snap)
	m.record(1, 0, mvDeltaExec(mvTestAddr, 105, 5))
	m.record(2, 0, mvDeltaExec(mvTestAddr, 112, 7))
	m.convertWritesToEstimates(2)

	_, res := m.resolveBalance(mvTestAddr, 3, nil)
	require.Equal(t, 2, res.blocking)

	val, res := m.resolveBalance(mvTestAddr, 2, nil)
	require.Equal(t, -1, res.blocking)
	require.Equal(t, uint64(105), val.Uint64())
	require.Equal(t, []txVersion{{txIdx: 1}}, res.fold)

	// A new incarnation replaces the estimate and readers resolve again.
	require.False(t, m.record(2, 1, mvDeltaExec(mvTestAddr, 115, 10)))
	val, res = m.resolveBalance(mvTestAddr, 3, nil)
	require.Equal(t, -1, res.blocking)
	require.Equal(t, uint64(115), val.Uint64())
	require.Equal(t, []txVersion{{txIdx: 2, incarnation: 1}, {txIdx: 1}}, res.fold)
}

func TestMVMemoryStorageClearOrdering(t *testing.T) {
	snap := NewMemoryState()
	snap.SetState(mvTestAddr, mvTestSlot, common.BytesToHash([]byte{3}))
	m := mvTestMemory(t, 6, snap)

	m.record(1, 0, mvStorageExec(mvTestAddr, map[common.Hash]common.Hash{mvTestSlot: common.BytesToHash([]byte{7})}, false))
	m.record(2, 0, mvStorageExec(mvTestAddr, nil, true))

	val, res := m.resolveStorage(mvTestAddr, mvTestSlot, 2)
	require.Equal(t, common.BytesToHash([]byte{7}), val)
	require.Equal(t, readOrigin{kind: originVersion, version: txVersion{txIdx: 1}}, res.origin)

	val, res = m.resolveStorage(mvTestAddr, mvTestSlot, 3)
	require.Equal(t, common.Hash{}, val)
	require.Equal(t, readOrigin{kind: originClear, version: txVersion{txIdx: 2}}, res.origin)

	// A slot never written is also zero under the clear.
	val, res = m.resolveStorage(mvTestAddr, mvTestSlot2, 3)
	require.Equal(t, common.Hash{}, val)
	require.Equal(t, originClear, res.origin.kind)

	// Clear and write in the same incarnation: the write wins.
	m.record(3, 0, mvStorageExec(mvTestAddr, map[common.Hash]common.Hash{mvTestSlot: common.BytesToHash([]byte{9})}, true))
	val, res = m.resolveStorage(mvTestAddr, mvTestSlot, 4)
	require.Equal(t, common.BytesToHash([]byte{9}), val)
	require.Equal(t, readOrigin{kind: originVersion, version: txVersion{txIdx: 3}}, res.origin)

	// An estimate on the clear blocks readers above it.
	m.convertWritesToEstimates(2)
	_, res = m.resolveStorage(mvTestAddr, mvTestSlot2, 3)
	require.Equal(t, 2, res.blocking)
}

func TestMVMemoryRecordRemovesOldWrites(t *testing.T) {
	snap := NewMemoryState()
	m := mvTestMemory(t, 3, snap)
	k1, k2, k3 := mvTestSlot, mvTestSlot2, common.BytesToHash([]byte{3})
	one := common.BytesToHash([]byte{1})

	require.True(t, m.record(1, 0, mvStorageExec(mvTestAddr, map[common.Hash]common.Hash{k1: one, k2: one}, false)))
	require.True(t, m.record(1, 1, mvStorageExec(mvTestAddr, map[common.Hash]common.Hash{k2: one, k3: one}, false)))
	val, res := m.resolveStorage(mvTestAddr, k1, 2)
	require.Equal(t, common.Hash{}, val)
	require.Equal(t, originSnapshot, res.origin.kind, "k1 was dropped by the second incarnation")
	_, res = m.resolveStorage(mvTestAddr, k3, 2)
	require.Equal(t, readOrigin{kind: originVersion, version: txVersion{txIdx: 1, incarnation: 1}}, res.origin)

	require.False(t, m.record(1, 2, mvStorageExec(mvTestAddr, map[common.Hash]common.Hash{k2: one}, false)))
	_, res = m.resolveStorage(mvTestAddr, k3, 2)
	require.Equal(t, originSnapshot, res.origin.kind)

	failed := mvStorageExec(mvTestAddr, map[common.Hash]common.Hash{k2: one}, false)
	failed.err = errors.New("nonce too high")
	require.False(t, m.record(1, 3, failed))
	_, res = m.resolveStorage(mvTestAddr, k2, 2)
	require.Equal(t, originSnapshot, res.origin.kind, "an erroring incarnation publishes nothing")
	_, _, recorded := m.readsOf(1)
	require.True(t, recorded)
}

func TestMVMemoryValidateReadSetModes(t *testing.T) {
	snap := NewMemoryState()
	snap.SetBalance(mvTestAddr2, big.NewInt(100))
	m := mvTestMemory(t, 5, snap)
	seven := common.BytesToHash([]byte{7})
	m.record(1, 0, mvStorageExec(mvTestAddr, map[common.Hash]common.Hash{mvTestSlot: seven}, false))

	// tx3 reads the slot (from tx1) and addr2's balance (from the snapshot).
	slotVal, slotRes := m.resolveStorage(mvTestAddr, mvTestSlot, 3)
	balVal, balRes := m.resolveBalance(mvTestAddr2, 3, nil)
	reads := []mvRead{
		{key: stateAccessKey{kind: stateAccessStorage, address: mvTestAddr, slot: mvTestSlot}, origin: slotRes.origin},
		{key: stateAccessKey{kind: stateAccessBalance, address: mvTestAddr2}, origin: balRes.origin, u256: balVal},
	}
	reads[0].u256.SetBytes32(slotVal[:])
	m.record(3, 0, &occTxExecution{reads: reads, writeSet: map[stateAccessKey]struct{}{}})

	ok, _, _ := m.validateReadSet(3, false)
	require.True(t, ok)
	ok, _, _ = m.validateReadSet(3, true)
	require.True(t, ok)

	// tx1 re-executes and writes the same value: version validation fails,
	// value validation passes.
	m.record(1, 1, mvStorageExec(mvTestAddr, map[common.Hash]common.Hash{mvTestSlot: seven}, false))
	ok, stale, count := m.validateReadSet(3, false)
	require.False(t, ok)
	require.Equal(t, 1, count)
	require.Equal(t, stateAccessStorage, stale[0].kind)
	ok, _, _ = m.validateReadSet(3, true)
	require.True(t, ok)

	// A new delta below tx3 changes addr2's balance: both modes fail.
	m.record(2, 0, mvDeltaExec(mvTestAddr2, 105, 5))
	ok, _, count = m.validateReadSet(3, true)
	require.False(t, ok)
	require.Equal(t, 1, count)
	ok, _, count = m.validateReadSet(3, false)
	require.False(t, ok)
	require.Equal(t, 2, count)

	// An estimate in the read path fails both modes.
	m.convertWritesToEstimates(2)
	ok, _, _ = m.validateReadSet(3, true)
	require.False(t, ok)

	// Never recorded transactions do not validate.
	ok, _, _ = m.validateReadSet(4, false)
	require.False(t, ok)
}

func TestMVMemoryResetDropsPreviousBlock(t *testing.T) {
	snapA := NewMemoryState()
	m := mvTestMemory(t, 2, snapA)
	m.record(1, 0, mvStorageExec(mvTestAddr, map[common.Hash]common.Hash{mvTestSlot: common.BytesToHash([]byte{7})}, true))
	require.True(t, m.hasClears.Load())

	snapB := NewMemoryState()
	snapB.SetState(mvTestAddr, mvTestSlot, common.BytesToHash([]byte{4}))
	m.reset(snapB, 3)
	require.False(t, m.hasClears.Load())
	val, res := m.resolveStorage(mvTestAddr, mvTestSlot, 2)
	require.Equal(t, common.BytesToHash([]byte{4}), val)
	require.Equal(t, originSnapshot, res.origin.kind)
	_, _, recorded := m.readsOf(1)
	require.False(t, recorded)
}

func TestMVMemoryChangeSetIntoMatchesBlockSTMState(t *testing.T) {
	snap := NewMemoryState()
	snap.SetBalance(mvTestAddr, big.NewInt(100))
	snap.SetBalance(mvTestAddr2, big.NewInt(30))
	snap.SetNonce(mvTestAddr2, 4)
	snap.SetState(mvTestAddr, mvTestSlot, common.BytesToHash([]byte{3}))
	snap.SetState(mvTestAddr2, mvTestSlot, common.BytesToHash([]byte{8}))

	execs := []*occTxExecution{
		mvDeltaExec(mvTestAddr, 105, 5),
		mvStorageExec(mvTestAddr, map[common.Hash]common.Hash{mvTestSlot: common.BytesToHash([]byte{7}), mvTestSlot2: common.BytesToHash([]byte{1})}, false),
		mvStorageExec(mvTestAddr, map[common.Hash]common.Hash{mvTestSlot2: common.BytesToHash([]byte{2})}, true),
		{
			changeSet: StateChangeSet{
				Nonces: []NonceChange{{Address: mvTestAddr2, Nonce: 5}},
				Code:   []CodeChange{{Address: mvTestAddr2, Code: []byte{0x60, 0x00}}},
			},
			writeSet: map[stateAccessKey]struct{}{},
		},
		// Writes the slot back to its snapshot value: no change emitted.
		mvStorageExec(mvTestAddr2, map[common.Hash]common.Hash{mvTestSlot: common.BytesToHash([]byte{8})}, false),
		mvDeltaExec(mvTestAddr, 112, 7),
	}

	m := mvTestMemory(t, len(execs), snap)
	prefix := newBlockSTMState(snap)
	for i, exec := range execs {
		m.record(i, 0, exec)
		prefix.apply(*exec)
	}

	var fromMV, fromPrefix StateChangeSet
	m.ChangeSetInto(&fromMV)
	prefix.ChangeSetInto(&fromPrefix)
	require.Equal(t, fromPrefix, fromMV)

	require.Len(t, fromMV.Balances, 1)
	require.Equal(t, big.NewInt(112), fromMV.Balances[0].Balance)
	require.Equal(t, []common.Address{mvTestAddr}, fromMV.StorageClears)
	// Slot 1 was written before the clear and never after: it is covered by the
	// clear, not emitted. Slot 2 was rewritten after the clear.
	require.Len(t, fromMV.Storage, 1)
	require.Equal(t, mvTestSlot2, fromMV.Storage[0].Key)
	require.Equal(t, common.BytesToHash([]byte{2}), fromMV.Storage[0].Value)
	require.Equal(t, []NonceChange{{Address: mvTestAddr2, Nonce: 5}}, fromMV.Nonces)
	require.Len(t, fromMV.Code, 1)
}

func TestMVMemoryChangeSetIntoPanicsOnEstimate(t *testing.T) {
	m := mvTestMemory(t, 2, NewMemoryState())
	m.record(1, 0, mvBalanceExec(mvTestAddr, 5))
	m.convertWritesToEstimates(1)
	require.Panics(t, func() {
		var cs StateChangeSet
		m.ChangeSetInto(&cs)
	})
}

func TestMVLocationUpsertKeepsOrder(t *testing.T) {
	loc := &mvLocation{}
	for _, idx := range []int32{5, 1, 3, 3, 9} {
		loc.upsert(mvEntry{version: txVersion{txIdx: idx}, kind: mvValue, nonce: uint64(idx)})
	}
	require.Len(t, loc.entries, 4)
	for i := 1; i < len(loc.entries); i++ {
		require.Less(t, loc.entries[i-1].version.txIdx, loc.entries[i].version.txIdx)
	}
	e, ok := loc.latestBelow(4)
	require.True(t, ok)
	require.Equal(t, int32(3), e.version.txIdx)
	_, ok = loc.latestBelow(1)
	require.False(t, ok)
	loc.remove(3)
	e, ok = loc.latestBelow(4)
	require.True(t, ok)
	require.Equal(t, int32(1), e.version.txIdx)
	loc.setEstimate(9)
	e, _ = loc.latestBelow(10)
	require.Equal(t, mvEstimate, e.kind)
}

func TestUint256FromBigOrMax(t *testing.T) {
	zero := uint256FromBigOrMax(nil)
	require.True(t, zero.IsZero())
	negative := uint256FromBigOrMax(big.NewInt(-1))
	require.True(t, negative.IsZero())
	seven := uint256FromBigOrMax(big.NewInt(7))
	require.Equal(t, uint64(7), seven.Uint64())
	huge := uint256FromBigOrMax(new(big.Int).Lsh(big.NewInt(1), 260))
	require.True(t, huge.Eq(new(uint256.Int).SetAllOne()))
}

// TestMVMemoryReuseAcrossBlocksKeepsLocationsDistinct reuses one memory for a
// second block and creates locations from many goroutines, as workers do. Every
// key must get its own version list; a shared list mixes values between keys.
func TestMVMemoryReuseAcrossBlocksKeepsLocationsDistinct(t *testing.T) {
	const keys = 512
	m := mvTestMemory(t, 1, NewMemoryState())
	for i := range keys {
		m.lookupOrCreate(stateAccessKey{kind: stateAccessStorage, address: mvTestAddr, slot: common.BigToHash(big.NewInt(int64(i)))})
	}
	m.reset(NewMemoryState(), 1)

	locs := make([]*mvLocation, keys)
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := w; i < keys; i += 8 {
				locs[i] = m.lookupOrCreate(stateAccessKey{kind: stateAccessNonce, address: common.BigToAddress(big.NewInt(int64(i + 1)))})
			}
		}()
	}
	wg.Wait()
	seen := make(map[*mvLocation]int, keys)
	for i, loc := range locs {
		require.NotNil(t, loc)
		if prev, dup := seen[loc]; dup {
			t.Fatalf("keys %d and %d share a location", prev, i)
		}
		seen[loc] = i
	}
}
