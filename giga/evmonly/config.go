package evmonly

import (
	"fmt"
	"math/big"
	"runtime"

	"github.com/ethereum/go-ethereum/params"

	"github.com/sei-protocol/sei-chain/giga/evmonly/precompiles"
)

// OCCMode selects the optimistic parallel execution engine used when
// Config.OCCWorkers > 1.
type OCCMode string

const (
	// OCCModeBlockSTM runs Block-STM: a collaborative scheduler over
	// multi-version memory with per-transaction read-set validation.
	OCCModeBlockSTM OCCMode = "blockstm"
	// OCCModeSnapshot runs every transaction against the block's base snapshot,
	// then validates and reruns conflicting transactions in index order.
	OCCModeSnapshot OCCMode = "snapshot"
)

// ParseOCCMode returns the OCCMode named by s. Empty selects OCCModeBlockSTM.
func ParseOCCMode(s string) (OCCMode, error) {
	switch OCCMode(s) {
	case "", OCCModeBlockSTM:
		return OCCModeBlockSTM, nil
	case OCCModeSnapshot:
		return OCCModeSnapshot, nil
	default:
		return "", fmt.Errorf("unsupported occ mode %q: want %q or %q", s, OCCModeBlockSTM, OCCModeSnapshot)
	}
}

// Config captures the sei-v3 executor knobs needed by the EVM-only path.
type Config struct {
	DisableNonceCheck    bool
	DisableGasPriceCheck bool
	MinGasPrice          *big.Int
	// ChainConfig defaults to params.AllDevChainProtocolChanges when nil. Test
	// and scaffold callers can use the default, but production wiring should pass
	// the chain's explicit config.
	ChainConfig       *params.ChainConfig
	CustomPrecompiles precompiles.Registry
	OCCWorkers        int
	// OCCMode selects the parallel engine used when OCCWorkers > 1. Empty
	// defaults to OCCModeBlockSTM.
	OCCMode OCCMode
	// OCCValueValidation makes Block-STM validate a read by comparing the value
	// it would return now with the value that was read, instead of comparing the
	// version it resolved to. Both are sound; value validation accepts more
	// incarnations at the cost of storing and comparing read values.
	OCCValueValidation bool
	// ParseWorkers controls parallel transaction decoding and sender recovery.
	// Values <= 0 default to GOMAXPROCS.
	ParseWorkers int
	// BlockResultPoolSize enables a bounded reusable output pool. Callers that
	// enable it must call BlockResult.Release when they are done with returned
	// results. Result sinks receive a retained result and must release it after
	// they finish async persistence. Pool exhaustion allocates an unpooled result
	// instead of blocking block execution.
	BlockResultPoolSize int
}

func DefaultConfig() Config {
	return Config{
		MinGasPrice:  big.NewInt(1_000_000_000),
		ParseWorkers: runtime.GOMAXPROCS(0),
	}
}

func (c Config) WithDefaults() Config {
	defaults := DefaultConfig()
	if c.MinGasPrice == nil {
		c.MinGasPrice = new(big.Int).Set(defaults.MinGasPrice)
	}
	if c.ParseWorkers <= 0 {
		c.ParseWorkers = defaults.ParseWorkers
	}
	if c.OCCMode == "" {
		c.OCCMode = OCCModeBlockSTM
	}
	return c
}
