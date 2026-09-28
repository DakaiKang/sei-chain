package evmonly

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
)

// occBlockSTMHooks are test-only observation points. Nil functions are no-ops.
type occBlockSTMHooks struct {
	beforeExecute  func(txIdx, incarnation int)
	afterExecute   func(txIdx, incarnation int)
	beforeRecord   func(txIdx, incarnation int)
	beforeValidate func(txIdx, incarnation int)
}

// occBlockSTMStats collects OCCStats inputs from concurrent workers.
type occBlockSTMStats struct {
	validationCount  atomic.Uint64
	rerunCount       atomic.Uint64
	maxIncarnation   atomic.Uint64
	conflictCount    atomic.Uint64
	dependencyAborts atomic.Uint64
	mu               sync.Mutex
	conflicts        map[occConflictAggregationKey]uint64
}

func (s *occBlockSTMStats) recordAbort(nextIncarnation int, stale []stateAccessKey, staleCount int) {
	s.rerunCount.Add(1)
	next := uint64(nextIncarnation) //nolint:gosec // bounded by occMaxTxIncarnations.
	for {
		cur := s.maxIncarnation.Load()
		if next <= cur || s.maxIncarnation.CompareAndSwap(cur, next) {
			break
		}
	}
	s.conflictCount.Add(uint64(staleCount)) //nolint:gosec // a count, never negative.
	if len(stale) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conflicts == nil {
		s.conflicts = map[occConflictAggregationKey]uint64{}
	}
	for _, k := range stale {
		s.conflicts[occConflictAggregationKey{access: "read", kind: k.kind, address: k.address, slot: k.slot}]++
	}
}

func (s *occBlockSTMStats) validationResult() occValidationResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	res := occValidationResult{
		rerunCount:       s.rerunCount.Load(),
		maxIncarnation:   s.maxIncarnation.Load(),
		conflictCount:    s.conflictCount.Load(),
		validationCount:  s.validationCount.Load(),
		dependencyAborts: s.dependencyAborts.Load(),
	}
	if len(s.conflicts) > 0 {
		res.conflicts = make(map[occConflictAggregationKey]uint64, len(s.conflicts))
		for k, v := range s.conflicts {
			res.conflicts[k] = v
		}
	}
	return res
}

// occBlockSTMRun is one block's Block-STM execution.
type occBlockSTMRun struct {
	executor *Executor
	runner   occSpeculativeRunner
	req      PreparedBlock
	mem      *mvMemory
	sched    *occScheduler
	// results holds the last execution of every transaction. Only the worker
	// holding the transaction's EXECUTING status writes a slot; finalize reads
	// them after every worker has returned.
	results []occTxExecution
	// prevSameSender[i] is the index of the closest lower transaction with the
	// same sender, or -1. A nonce or funds error on a stale read is turned into
	// a dependency on it instead of a re-execution.
	prevSameSender []int
	stats          occBlockSTMStats
	hooks          occBlockSTMHooks
}

func sameSenderPredecessors(txs []PreparedTx) []int {
	prev := make([]int, len(txs))
	last := make(map[common.Address]int, len(txs))
	for i, tx := range txs {
		if j, ok := last[tx.Sender]; ok {
			prev[i] = j
		} else {
			prev[i] = -1
		}
		last[tx.Sender] = i
	}
	return prev
}

// isStaleSenderError reports whether a transaction-level error can be caused
// by reading the sender's nonce or balance before a lower transaction of the
// same sender has published.
func isStaleSenderError(err error) bool {
	return errors.Is(err, core.ErrNonceTooHigh) ||
		errors.Is(err, core.ErrNonceTooLow) ||
		errors.Is(err, core.ErrInsufficientFunds) ||
		errors.Is(err, core.ErrInsufficientFundsForTransfer)
}

// executeBlockBlockSTM runs the Block-STM engine: a collaborative scheduler over
// multi-version memory, with in-order finalisation once every transaction has
// been validated after its last execution.
func (e *Executor) executeBlockBlockSTM(ctx context.Context, req PreparedBlock, source StateReader) (*BlockResult, error) {
	n := len(req.Txs)
	mem := e.acquireMVMemory(source, n)
	defer e.releaseMVMemory(mem)
	runner := newOCCSpeculativeRunner(e, req)
	runner.prepareSenderHints(source)
	run := &occBlockSTMRun{
		executor:       e,
		runner:         runner,
		req:            req,
		mem:            mem,
		sched:          newOCCScheduler(n),
		results:        make([]occTxExecution, n),
		prevSameSender: sameSenderPredecessors(req.Txs),
		hooks:          e.occHooks,
	}
	err := e.occPool.Run(ctx, n, func(workerCtx context.Context, _ int, _ int) error {
		return run.workerLoop(workerCtx)
	})
	switch {
	case errors.Is(err, errOCCWorkerPoolClosed):
		return e.executeBlockOCCSequentialFallback(ctx, req, source, run.stats.validationResult(), occFallbackReasonWorkerPoolClosed)
	case errors.Is(err, errOCCMaxIncarnation):
		return e.executeBlockOCCSequentialFallback(ctx, req, source, run.stats.validationResult(), occFallbackReasonMaxIncarnation)
	case err != nil:
		return nil, err
	}
	result, err := run.finalize(ctx)
	if errors.Is(err, errOCCNonceHint) {
		return e.executeBlockOCCSequentialFallback(ctx, req, source, run.stats.validationResult(), occFallbackReasonNonceHint)
	}
	return result, err
}

func (r *occBlockSTMRun) workerLoop(ctx context.Context) error {
	var task occTask
	idle := 0
	for !r.sched.isDone() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if task.kind == occTaskNone {
			task = r.sched.nextTask()
			if task.kind == occTaskNone {
				// Nothing to dispatch right now: another worker holds the task that
				// will unblock progress. Back off briefly instead of spinning.
				if idle < 64 {
					runtime.Gosched()
				} else {
					time.Sleep(20 * time.Microsecond)
				}
				idle++
				continue
			}
			idle = 0
		}
		var err error
		switch task.kind {
		case occTaskExecute:
			task, err = r.executeTask(ctx, task)
		case occTaskValidate:
			task, err = r.validateTask(task)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *occBlockSTMRun) executeTask(ctx context.Context, task occTask) (occTask, error) {
	waitedForSender := false
	for {
		if r.hooks.beforeExecute != nil {
			r.hooks.beforeExecute(task.txIdx, task.incarnation)
		}
		exec, err := r.executor.executeIncarnation(
			ctx,
			r.mem,
			r.req,
			task.txIdx,
			uint(task.txIdx), //nolint:gosec // bounded by the block's transaction count.
			task.incarnation,
			r.runner.chainConfig,
			r.runner.blockCtx,
			r.runner.baseFee,
			r.runner.blockGasLimit,
			r.runner.senderHint(task.txIdx),
		)
		if r.hooks.afterExecute != nil {
			r.hooks.afterExecute(task.txIdx, task.incarnation)
		}
		if err != nil {
			var abort *mvEstimateAbort
			if !errors.As(err, &abort) {
				return occTask{}, err
			}
			r.stats.dependencyAborts.Add(1)
			if r.sched.addDependency(task.txIdx, abort.blockingTxIdx) {
				return occTask{}, nil
			}
			// The blocking transaction finished in the meantime: retry immediately.
			continue
		}
		// A nonce or funds error is usually a stale read of the sender's state
		// before its previous transaction published. Waiting for that transaction
		// costs one dependency instead of one re-execution per lower publish. The
		// wait happens at most once so a genuine error is still recorded.
		if exec.err != nil && !waitedForSender && isStaleSenderError(exec.err) {
			if prev := r.prevSameSender[task.txIdx]; prev >= 0 {
				waitedForSender = true
				r.stats.dependencyAborts.Add(1)
				if r.sched.addDependency(task.txIdx, prev) {
					return occTask{}, nil
				}
				continue
			}
		}
		if r.hooks.beforeRecord != nil {
			r.hooks.beforeRecord(task.txIdx, task.incarnation)
		}
		wroteNew := r.mem.record(task.txIdx, task.incarnation, &exec)
		r.results[task.txIdx] = exec
		return r.sched.finishExecution(task.txIdx, task.incarnation, wroteNew), nil
	}
}

func (r *occBlockSTMRun) validateTask(task occTask) (occTask, error) {
	if r.hooks.beforeValidate != nil {
		r.hooks.beforeValidate(task.txIdx, task.incarnation)
	}
	r.stats.validationCount.Add(1)
	ok, stale, staleCount := r.mem.validateReadSet(task.txIdx, r.executor.cfg.OCCValueValidation)
	aborted := false
	if !ok && r.sched.tryValidationAbort(task.txIdx, task.incarnation) {
		aborted = true
		r.stats.recordAbort(task.incarnation+1, stale, staleCount)
		r.mem.convertWritesToEstimates(task.txIdx)
	}
	return r.sched.finishValidation(task.txIdx, task.incarnation, aborted)
}

// finalize walks the transactions in block order, enforcing the block gas
// limit exactly as the sequential engine's shared gas pool does and surfacing
// the first transaction-level error, then consolidates multi-version memory
// into the block changeset.
//
// Commit is post-hoc for now; an incremental commit cursor advanced from
// finishValidation is the natural hook for streaming receipts later.
func (r *occBlockSTMRun) finalize(ctx context.Context) (*BlockResult, error) {
	if !r.sched.allValidated() {
		return nil, errOCCSchedulerInvariant
	}
	var cumulative uint64
	limit := r.runner.blockGasLimit
	for i := range r.results {
		res := &r.results[i]
		if res.err != nil {
			return nil, res.err
		}
		if res.gasUsed > math.MaxUint64-cumulative {
			return nil, errors.New(occFallbackReasonGasOverflow)
		}
		if cumulative > limit || res.gasLimit > limit-cumulative {
			return nil, fmt.Errorf("execute tx %d %s: %w", i, r.req.Txs[i].Tx.Hash(), core.ErrGasLimitReached)
		}
		if hint := r.runner.senderHint(i); hint.active {
			// Verify the nonce premise: every transaction saw the nonce the block
			// order predicts. A mismatch means another transaction changed this
			// sender's nonce and the block must be re-executed without predictions.
			nonce, resolution := r.mem.resolveNonce(r.req.Txs[i].Sender, i)
			if resolution.blocking >= 0 || nonce != hint.nonce {
				return nil, errOCCNonceHint
			}
		}
		cumulative += res.gasUsed
	}
	result, err := r.executor.mergeOCCResults(ctx, r.results, r.mem)
	if err != nil {
		return nil, err
	}
	result.OCCStats = r.stats.validationResult().stats(false)
	return result, nil
}
