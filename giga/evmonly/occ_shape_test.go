package evmonly

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
)

// TestMVMemoryValidateAccountShapeRead: an existence check depends on the
// account's shape, so commutative credits below it do not invalidate it while
// a write that empties the account does.
func TestMVMemoryValidateAccountShapeRead(t *testing.T) {
	snap := NewMemoryState()
	snap.SetBalance(mvTestAddr, big.NewInt(100))
	m := mvTestMemory(t, 5, snap)
	m.record(1, 0, mvDeltaExec(mvTestAddr, 105, 5))

	read := mvRead{
		key:   stateAccessKey{kind: stateAccessAccount, address: mvTestAddr},
		shape: accountShape{nonceZero: true, balanceZero: false, codeEmpty: true},
	}
	m.record(3, 0, &occTxExecution{reads: []mvRead{read}, writeSet: map[stateAccessKey]struct{}{}})
	ok, _, _ := m.validateReadSet(3, false)
	require.True(t, ok, "a credit keeps the account non-empty")

	m.record(2, 0, mvDeltaExec(mvTestAddr, 112, 7))
	ok, _, _ = m.validateReadSet(3, false)
	require.True(t, ok, "another credit still keeps the shape")

	m.record(2, 1, mvBalanceExec(mvTestAddr, 0))
	ok, stale, _ := m.validateReadSet(3, false)
	require.False(t, ok, "emptying the account changes its shape")
	require.Equal(t, stateAccessAccount, stale[0].kind)
}

// TestMVReaderShapeReadsCollectAsAccountKind: an account read through the
// versioned reader is collected as one shape read rather than three exact reads,
// and a pending estimate on any of the three fields aborts it.
func TestMVReaderShapeReadsCollectAsAccountKind(t *testing.T) {
	snap := NewMemoryState()
	snap.SetBalance(mvTestAddr, big.NewInt(100))
	m := mvTestMemory(t, 3, snap)
	m.record(0, 0, mvDeltaExec(mvTestAddr, 105, 5))

	reader := newMVVersionedReader()
	reader.reset(m, 2, 0)
	// nativeStateDB.loadAccount loads all three fields, then Exist marks the read.
	_ = reader.GetBalance(mvTestAddr)
	_ = reader.GetNonce(mvTestAddr)
	_ = reader.GetCode(mvTestAddr)
	reader.observeRead(stateAccessKey{kind: stateAccessAccount, address: mvTestAddr})
	reads := reader.collect()
	require.Len(t, reads, 1)
	require.Equal(t, stateAccessAccount, reads[0].key.kind)
	require.Equal(t, accountShape{nonceZero: true, balanceZero: false, codeEmpty: true}, reads[0].shape)

	// With the credit converted to an estimate, the shape read must abort.
	m.convertWritesToEstimates(0)
	reader.reset(m, 2, 0)
	_ = reader.GetBalance(mvTestAddr) // resolves to an estimate, not yet needed
	require.PanicsWithError(t, (&mvEstimateAbort{txIdx: 2, blockingTxIdx: 0, key: stateAccessKey{kind: stateAccessBalance, address: mvTestAddr}}).Error(), func() {
		reader.observeRead(stateAccessKey{kind: stateAccessAccount, address: mvTestAddr})
	})
}

// TestSnapshotEngineValidatesAccountReadsByShape: the snapshot engine compares an
// account read's observed shape against the prefix instead of the write index.
func TestSnapshotEngineValidatesAccountReadsByShape(t *testing.T) {
	addr := common.BigToAddress(big.NewInt(0xabc))
	source := NewMemoryState()
	source.SetBalance(addr, big.NewInt(100))
	prefix := newBlockSTMState(source)
	prefix.apply(*mvDeltaExec(addr, 105, 5))

	writes := newStateAccessIndex()
	writes.addCommutativeBalanceDeltasAt(0, map[common.Address]*big.Int{addr: big.NewInt(5)})
	result := occTxExecution{
		gasLimit:      1,
		gasUsed:       1,
		readSet:       map[stateAccessKey]struct{}{{kind: stateAccessAccount, address: addr}: {}},
		accountShapes: map[common.Address]accountShape{addr: {nonceZero: true, balanceZero: false, codeEmpty: true}},
	}
	validation := occValidationResult{}
	require.True(t, validateSTMResultAgainstPrefix(&validation, writes, result, 0, 10, 0, prefix), "credit keeps the shape")
	require.False(t, validateSTMResultAgainstPrefix(&validation, writes, result, 0, 10, 0, nil), "without a prefix the index rule applies")

	prefix.apply(*mvBalanceExec(addr, 0))
	validation = occValidationResult{}
	require.False(t, validateSTMResultAgainstPrefix(&validation, writes, result, 0, 10, 0, prefix), "emptying changes the shape")
	require.Equal(t, uint64(1), validation.conflictCount)
}
