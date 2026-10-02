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

// Storage operations a storage-rw transaction performs on the slot it selects.
const (
	StorageOpRMW   = "rmw"
	StorageOpRead  = "read"
	StorageOpWrite = "write"
)

var (
	// PUSH1 0, CALLDATALOAD, DUP1, SLOAD, PUSH1 1, ADD, SWAP1, SSTORE, STOP:
	// the slot's new value depends on its old one, so two transactions on one
	// slot do not commute.
	storageRMWRuntimeCode = common.FromHex("0x6000358054600101905500")
	// PUSH1 0, CALLDATALOAD, SLOAD, POP, STOP.
	storageReadRuntimeCode = common.FromHex("0x600035545000")
	// PUSH1 32, CALLDATALOAD, PUSH1 0, CALLDATALOAD, SSTORE, STOP: stores the
	// caller-supplied word, so the result does not depend on the old value.
	storageWriteRuntimeCode = common.FromHex("0x6020356000355500")
)

// StorageRWWorkload sends every transaction to one contract that reads and
// writes a storage slot the caller names. Transactions share a keyspace of
// Config.StorageRecords slots, assigned by position within the block, so a
// block of T transactions over N slots has exactly T-N of them contending
// when N < T and none when N >= T. Storage conflicts are not commutative, so
// no delta or guard compression applies to them.
type StorageRWWorkload struct {
	cfg           Config
	state         State
	signer        ethtypes.Signer
	accountCursor atomic.Uint64
}

func NewStorageRWWorkload(cfg Config, state State) (*StorageRWWorkload, error) {
	if cfg.StorageRecords <= 0 {
		return nil, fmt.Errorf("storage-records must be positive")
	}
	code, err := storageRuntimeCode(cfg.StorageOp)
	if err != nil {
		return nil, err
	}
	state.SetCode(cfg.StorageContract, code)
	return &StorageRWWorkload{
		cfg:    cfg,
		state:  state,
		signer: ethtypes.LatestSignerForChainID(cfg.ChainID),
	}, nil
}

func storageRuntimeCode(op string) ([]byte, error) {
	switch op {
	case StorageOpRMW:
		return storageRMWRuntimeCode, nil
	case StorageOpRead:
		return storageReadRuntimeCode, nil
	case StorageOpWrite:
		return storageWriteRuntimeCode, nil
	default:
		return nil, fmt.Errorf("unsupported storage operation %q", op)
	}
}

func (w *StorageRWWorkload) BuildBlock(ctx context.Context, number uint64) (evmonly.BlockRequest, error) {
	txs := make([][]byte, w.cfg.TxsPerBlock)
	for i := 0; i < w.cfg.TxsPerBlock; i++ {
		select {
		case <-ctx.Done():
			return evmonly.BlockRequest{}, ctx.Err()
		default:
		}
		accountIndex := w.accountCursor.Add(1)
		raw, sender, err := w.buildCallTx(accountIndex, w.Slot(i))
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

// Slot returns the storage slot the transaction at txIndex within a block
// addresses.
func (w *StorageRWWorkload) Slot(txIndex int) common.Hash {
	return common.BigToHash(new(big.Int).SetUint64(uint64FromNonNegativeInt(txIndex) % uint64FromNonNegativeInt(w.cfg.StorageRecords)))
}

func (w *StorageRWWorkload) buildCallTx(accountIndex uint64, slot common.Hash) ([]byte, common.Address, error) {
	key, err := DeterministicPrivateKey(accountIndex)
	if err != nil {
		return nil, common.Address{}, err
	}
	sender := crypto.PubkeyToAddress(key.PublicKey)
	tx := ethtypes.NewTx(&ethtypes.LegacyTx{
		Nonce:    0,
		GasPrice: new(big.Int).Set(w.cfg.GasPrice),
		Gas:      w.cfg.TxGasLimit,
		To:       &w.cfg.StorageContract,
		Value:    new(big.Int),
		Data:     storageCalldata(slot, accountIndex),
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

// storageCalldata is the slot followed by the word a write stores, which varies
// per sender so repeated writes are not no-ops.
func storageCalldata(slot common.Hash, value uint64) []byte {
	data := make([]byte, 64)
	copy(data[:32], slot.Bytes())
	copy(data[32:], common.BigToHash(new(big.Int).SetUint64(value+1)).Bytes())
	return data
}
