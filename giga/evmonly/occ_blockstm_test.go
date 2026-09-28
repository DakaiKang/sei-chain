package evmonly

import (
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/require"
)

// holdFirstTxWritesUntilLastTxExecuted delays publishing tx 0's first
// incarnation until the last transaction's first incarnation has executed, so
// fixtures whose conflict runs through tx 0 always observe stale state once.
// It needs at least two workers.
func holdFirstTxWritesUntilLastTxExecuted(txCount int) occBlockSTMHooks {
	released := make(chan struct{})
	var once sync.Once
	return occBlockSTMHooks{
		afterExecute: func(txIdx, incarnation int) {
			if txIdx == txCount-1 && incarnation == 0 {
				once.Do(func() { close(released) })
			}
		},
		beforeRecord: func(txIdx, incarnation int) {
			if txIdx == 0 && incarnation == 0 {
				select {
				case <-released:
				case <-time.After(10 * time.Second):
				}
			}
		},
	}
}

// randomScheduleDelays perturbs worker interleaving with seeded sleeps.
func randomScheduleDelays(seed int64) occBlockSTMHooks {
	var mu sync.Mutex
	rng := rand.New(rand.NewSource(seed))
	sleep := func(int, int) {
		mu.Lock()
		d := time.Duration(rng.Intn(200)) * time.Microsecond
		mu.Unlock()
		time.Sleep(d)
	}
	return occBlockSTMHooks{beforeExecute: sleep, beforeRecord: sleep, beforeValidate: sleep}
}

func deterministicKey(t *testing.T, seed int64, index int) *ecdsa.PrivateKey {
	t.Helper()
	key, err := crypto.ToECDSA(crypto.Keccak256([]byte(fmt.Sprintf("blockstm-test-%d-%d", seed, index))))
	require.NoError(t, err)
	return key
}

// blockSTMFixture builds a block and the two identical states it runs on.
type blockSTMFixture struct {
	name string
	// build returns raw txs and populates both states identically.
	build func(t *testing.T, seed int64, seq, occ *MemoryState) [][]byte
}

func fundBoth(seq, occ *MemoryState, addr common.Address, amount int64) {
	seq.SetBalance(addr, big.NewInt(amount))
	occ.SetBalance(addr, big.NewInt(amount))
}

var blockSTMFixtures = []blockSTMFixture{
	{
		name: "hot-recipient",
		build: func(t *testing.T, seed int64, seq, occ *MemoryState) [][]byte {
			chainID := big.NewInt(testChainID)
			recipient := common.BigToAddress(big.NewInt(0xf1))
			txs := make([][]byte, 0, 32)
			for i := range 32 {
				key := deterministicKey(t, seed, i)
				fundBoth(seq, occ, crypto.PubkeyToAddress(key.PublicKey), 1_000_000_000)
				txs = append(txs, signLegacyTxWithGasPrice(t, key, chainID, 0, &recipient, big.NewInt(int64(i+1)), nil, 100_000, big.NewInt(1)))
			}
			return txs
		},
	},
	{
		name: "same-sender",
		build: func(t *testing.T, seed int64, seq, occ *MemoryState) [][]byte {
			chainID := big.NewInt(testChainID)
			key := deterministicKey(t, seed, 0)
			fundBoth(seq, occ, crypto.PubkeyToAddress(key.PublicKey), 1_000_000_000)
			txs := make([][]byte, 0, 16)
			for i := range 16 {
				recipient := common.BigToAddress(big.NewInt(int64(0x1000 + i)))
				txs = append(txs, signLegacyTxWithGasPrice(t, key, chainID, uint64(i), &recipient, big.NewInt(1), nil, 100_000, big.NewInt(1)))
			}
			return txs
		},
	},
	{
		name: "conflict-pairs",
		build: func(t *testing.T, seed int64, seq, occ *MemoryState) [][]byte {
			chainID := big.NewInt(testChainID)
			txs := make([][]byte, 0, 24)
			for i := range 24 {
				key := deterministicKey(t, seed, i)
				fundBoth(seq, occ, crypto.PubkeyToAddress(key.PublicKey), 1_000_000_000)
				recipient := common.BigToAddress(big.NewInt(int64(0x2000 + i/2)))
				txs = append(txs, signLegacyTxWithGasPrice(t, key, chainID, 0, &recipient, big.NewInt(3), nil, 100_000, big.NewInt(0)))
			}
			return txs
		},
	},
	{
		name: "mixed-senders-and-recipients",
		build: func(t *testing.T, seed int64, seq, occ *MemoryState) [][]byte {
			chainID := big.NewInt(testChainID)
			keys := make([]*ecdsa.PrivateKey, 6)
			for i := range keys {
				keys[i] = deterministicKey(t, seed, i)
				fundBoth(seq, occ, crypto.PubkeyToAddress(keys[i].PublicKey), 1_000_000_000)
			}
			nonces := make([]uint64, len(keys))
			rng := rand.New(rand.NewSource(seed))
			txs := make([][]byte, 0, 40)
			for range 40 {
				s := rng.Intn(len(keys))
				var recipient common.Address
				if rng.Intn(2) == 0 {
					recipient = crypto.PubkeyToAddress(keys[rng.Intn(len(keys))].PublicKey)
				} else {
					recipient = common.BigToAddress(big.NewInt(int64(0x3000 + rng.Intn(4))))
				}
				txs = append(txs, signLegacyTxWithGasPrice(t, keys[s], chainID, nonces[s], &recipient, big.NewInt(int64(1+rng.Intn(100))), nil, 100_000, big.NewInt(int64(rng.Intn(3)))))
				nonces[s]++
			}
			return txs
		},
	},
}

func requireBlockResultsEqual(t *testing.T, seq, occ *BlockResult) {
	t.Helper()
	require.Equal(t, seq.GasUsed, occ.GasUsed)
	require.Equal(t, seq.Txs, occ.Txs)
	require.Equal(t, seq.Receipts, occ.Receipts)
	require.Equal(t, seq.ChangeSet, occ.ChangeSet)
}

func TestExecutorBlockSTMMatchesSequential(t *testing.T) {
	for _, fixture := range blockSTMFixtures {
		for _, workers := range []int{2, 3, 4, 8} {
			for seed := int64(1); seed <= 3; seed++ {
				t.Run(fmt.Sprintf("%s/workers=%d/seed=%d", fixture.name, workers, seed), func(t *testing.T) {
					seqState, occState := NewMemoryState(), NewMemoryState()
					txs := fixture.build(t, seed, seqState, occState)
					req := BlockRequest{Context: blockContext(big.NewInt(testChainID)), Txs: txs}
					seqResult, err := NewExecutor(Config{MinGasPrice: big.NewInt(0)}, withTestState(seqState)).ExecuteBlock(t.Context(), req)
					require.NoError(t, err)
					occResult, err := NewExecutor(Config{MinGasPrice: big.NewInt(0), OCCWorkers: workers, OCCMode: OCCModeBlockSTM}, withTestState(occState)).ExecuteBlock(t.Context(), req)
					require.NoError(t, err)
					require.True(t, occResult.OCCStats.Attempted)
					require.False(t, occResult.OCCStats.Fallback)
					requireBlockResultsEqual(t, seqResult, occResult)
				})
			}
		}
	}
}

func TestExecutorBlockSTMValueValidationMatchesSequential(t *testing.T) {
	for _, fixture := range blockSTMFixtures {
		t.Run(fixture.name, func(t *testing.T) {
			seqState, occState := NewMemoryState(), NewMemoryState()
			txs := fixture.build(t, 7, seqState, occState)
			req := BlockRequest{Context: blockContext(big.NewInt(testChainID)), Txs: txs}
			seqResult, err := NewExecutor(Config{MinGasPrice: big.NewInt(0)}, withTestState(seqState)).ExecuteBlock(t.Context(), req)
			require.NoError(t, err)
			occResult, err := NewExecutor(Config{MinGasPrice: big.NewInt(0), OCCWorkers: 4, OCCValueValidation: true}, withTestState(occState)).ExecuteBlock(t.Context(), req)
			require.NoError(t, err)
			require.False(t, occResult.OCCStats.Fallback)
			requireBlockResultsEqual(t, seqResult, occResult)
		})
	}
}

func TestExecutorBlockSTMScheduleFuzz(t *testing.T) {
	seeds := int64(32)
	if testing.Short() {
		seeds = 4
	}
	for _, fixture := range blockSTMFixtures {
		seqState := NewMemoryState()
		scratch := NewMemoryState()
		txs := fixture.build(t, 11, seqState, scratch)
		req := BlockRequest{Context: blockContext(big.NewInt(testChainID)), Txs: txs}
		seqResult, err := NewExecutor(Config{MinGasPrice: big.NewInt(0)}, withTestState(seqState)).ExecuteBlock(t.Context(), req)
		require.NoError(t, err)
		for seed := int64(0); seed < seeds; seed++ {
			workers := []int{2, 3, 4, 8}[seed%4]
			t.Run(fmt.Sprintf("%s/seed=%d/workers=%d", fixture.name, seed, workers), func(t *testing.T) {
				occState := NewMemoryState()
				_ = fixture.build(t, 11, NewMemoryState(), occState)
				executor := NewExecutor(Config{MinGasPrice: big.NewInt(0), OCCWorkers: workers}, withTestState(occState))
				executor.occHooks = randomScheduleDelays(seed)
				occResult, err := executor.ExecuteBlock(t.Context(), req)
				require.NoError(t, err)
				require.False(t, occResult.OCCStats.Fallback)
				requireBlockResultsEqual(t, seqResult, occResult)
			})
		}
	}
}

func TestExecutorBlockSTMConflictingBlockDeterministicOutput(t *testing.T) {
	var baseline *BlockResult
	for run := range 8 {
		seqState, occState := NewMemoryState(), NewMemoryState()
		txs := blockSTMFixtures[0].build(t, 5, seqState, occState)
		req := BlockRequest{Context: blockContext(big.NewInt(testChainID)), Txs: txs}
		result, err := NewExecutor(Config{MinGasPrice: big.NewInt(0), OCCWorkers: 4}, withTestState(occState)).ExecuteBlock(t.Context(), req)
		require.NoError(t, err)
		if run == 0 {
			baseline = result
			continue
		}
		requireBlockResultsEqual(t, baseline, result)
	}
}

func TestExecutorBlockSTMSnapshotModeStillWorks(t *testing.T) {
	seqState, occState := NewMemoryState(), NewMemoryState()
	txs := blockSTMFixtures[3].build(t, 9, seqState, occState)
	req := BlockRequest{Context: blockContext(big.NewInt(testChainID)), Txs: txs}
	seqResult, err := NewExecutor(Config{MinGasPrice: big.NewInt(0)}, withTestState(seqState)).ExecuteBlock(t.Context(), req)
	require.NoError(t, err)
	occResult, err := NewExecutor(Config{MinGasPrice: big.NewInt(0), OCCWorkers: 4, OCCMode: OCCModeSnapshot}, withTestState(occState)).ExecuteBlock(t.Context(), req)
	require.NoError(t, err)
	require.True(t, occResult.OCCStats.Attempted)
	requireBlockResultsEqual(t, seqResult, occResult)
}

func TestSameSenderPredecessors(t *testing.T) {
	a := common.BigToAddress(big.NewInt(1))
	b := common.BigToAddress(big.NewInt(2))
	txs := []PreparedTx{{Sender: a}, {Sender: b}, {Sender: a}, {Sender: a}, {Sender: b}}
	require.Equal(t, []int{-1, -1, 0, 2, 1}, sameSenderPredecessors(txs))
}
