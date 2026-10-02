package scenarios

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/sei-protocol/sei-chain/giga/evmonly"
)

const (
	WorkloadTransfer       = "transfer"
	WorkloadERC20Transfer  = "erc20-transfer"
	WorkloadSnapshotRevert = "snapshot-revert"
	WorkloadStorageRW      = "storage-rw"
	WorkloadDivergentRW    = "divergent-rw"

	DefaultGenesisTimestamp = uint64(1_700_000_000)
)

type Config struct {
	TxsPerBlock            int
	ChainID                *big.Int
	GasPrice               *big.Int
	SenderBalance          *big.Int
	TransferValue          *big.Int
	TxGasLimit             uint64
	BlockGasLimit          uint64
	Coinbase               common.Address
	ERC20Contract          common.Address
	SnapshotRevertContract common.Address
	SnapshotRevertHelper   common.Address
	StorageContract        common.Address
	// StorageRecords is the number of distinct slots storage-rw transactions
	// share. A block of T transactions over N slots has T-N of them contending
	// when N < T.
	StorageRecords int
	// StorageOp is the operation each storage-rw transaction performs: rmw,
	// read, or write.
	StorageOp         string
	DivergentContract common.Address
	// DivergentFanout is the number of hot counter slots divergent-rw
	// transactions share. A block of T transactions over F counters forms F
	// chains of T/F.
	DivergentFanout int
	// DivergentTargetSpace is the number of distinct slots the derived writes
	// spread over.
	DivergentTargetSpace int
	// DivergentOp selects the arm: divergent, whose written slot is derived
	// during execution, or control, whose written slot comes from calldata.
	DivergentOp           string
	FixedRecipient        *common.Address
	RecipientConflictRate float64
	SameSender            bool
}

type State interface {
	SetBalance(common.Address, *big.Int)
	SetCode(common.Address, []byte)
	SetState(common.Address, common.Hash, common.Hash)
}

type Workload interface {
	BuildBlock(context.Context, uint64) (evmonly.BlockRequest, error)
}

func NewWorkload(kind string, cfg Config, state State) (Workload, error) {
	switch kind {
	case WorkloadTransfer:
		return NewTransferWorkload(cfg, state)
	case WorkloadERC20Transfer:
		return NewERC20TransferWorkload(cfg, state)
	case WorkloadSnapshotRevert:
		return NewSnapshotRevertWorkload(cfg, state), nil
	case WorkloadStorageRW:
		return NewStorageRWWorkload(cfg, state)
	case WorkloadDivergentRW:
		return NewDivergentRWWorkload(cfg, state)
	default:
		return nil, fmt.Errorf("unsupported workload %q", kind)
	}
}
