package evmonly

import (
	"errors"
	"sync"
	"sync/atomic"
)

// The Block-STM collaborative scheduler (Gelashvili et al., Algorithms 3-5)
// with the VALIDATED status pevm added for bookkeeping. Two shared counters hand
// out work: executionIdx is the next transaction to execute, validationIdx the
// next to validate. Validation is preferred whenever it lags execution. Aborts
// rewind the counters; the block is done when both counters have passed the
// end, no task is in flight, and no rewind happened while that was observed.

type occTxStatus uint8

const (
	occTxReady occTxStatus = iota // READY_TO_EXECUTE(incarnation)
	occTxExecuting
	occTxExecuted
	occTxValidated // passed validation since its last execution
	occTxAborting
)

type occTaskKind uint8

const (
	occTaskNone occTaskKind = iota
	occTaskExecute
	occTaskValidate
)

type occTask struct {
	kind        occTaskKind
	txIdx       int
	incarnation int
}

// occTxSlot is one transaction's scheduler state. Compound check-then-set
// transitions and the dependents list share one mutex. Slots are only ever
// locked in ascending txIdx order (a blocking transaction precedes its
// dependents), so the lock order is acyclic.
type occTxSlot struct {
	mu          sync.Mutex
	status      occTxStatus
	incarnation int
	dependents  []int
}

var errOCCSchedulerInvariant = errors.New("occ scheduler finished with unvalidated transactions")

type occScheduler struct {
	n             int
	executionIdx  atomic.Int64
	validationIdx atomic.Int64
	numActive     atomic.Int64 // dispatched tasks not yet finished
	decreaseCnt   atomic.Int64 // bumped on every counter rewind
	numValidated  atomic.Int64
	done          atomic.Bool
	txs           []occTxSlot
}

func newOCCScheduler(n int) *occScheduler {
	return &occScheduler{n: n, txs: make([]occTxSlot, n)}
}

func fetchMin(a *atomic.Int64, target int64) {
	for {
		cur := a.Load()
		if cur <= target || a.CompareAndSwap(cur, target) {
			return
		}
	}
}

func (s *occScheduler) decreaseExecutionIdx(target int) {
	fetchMin(&s.executionIdx, int64(target))
	s.decreaseCnt.Add(1)
}

func (s *occScheduler) decreaseValidationIdx(target int) {
	fetchMin(&s.validationIdx, int64(target))
	s.decreaseCnt.Add(1)
}

// isDone applies the paper's completion rule. The rewind counter is read
// before and after the index and active-task checks so a rewind that races
// with the observation forces another round.
func (s *occScheduler) isDone() bool {
	if s.done.Load() {
		return true
	}
	observed := s.decreaseCnt.Load()
	if min(s.executionIdx.Load(), s.validationIdx.Load()) >= int64(s.n) &&
		s.numActive.Load() == 0 &&
		s.decreaseCnt.Load() == observed {
		s.done.Store(true)
		return true
	}
	return false
}

// allValidated is a post-run invariant: every transaction passed validation
// after its final execution.
func (s *occScheduler) allValidated() bool {
	return s.numValidated.Load() == int64(s.n)
}

// tryIncarnate claims the transaction for execution. The caller has already
// counted an active task; on failure that count is released here.
func (s *occScheduler) tryIncarnate(idx int) occTask {
	if idx < s.n {
		slot := &s.txs[idx]
		slot.mu.Lock()
		if slot.status == occTxReady {
			slot.status = occTxExecuting
			task := occTask{kind: occTaskExecute, txIdx: idx, incarnation: slot.incarnation}
			slot.mu.Unlock()
			return task
		}
		slot.mu.Unlock()
	}
	s.numActive.Add(-1)
	return occTask{}
}

func (s *occScheduler) nextToExecute() occTask {
	if s.executionIdx.Load() >= int64(s.n) {
		return occTask{}
	}
	s.numActive.Add(1)
	idx := int(s.executionIdx.Add(1) - 1)
	return s.tryIncarnate(idx)
}

func (s *occScheduler) nextToValidate() occTask {
	if s.validationIdx.Load() >= int64(s.n) {
		return occTask{}
	}
	s.numActive.Add(1)
	idx := int(s.validationIdx.Add(1) - 1)
	if idx < s.n {
		slot := &s.txs[idx]
		slot.mu.Lock()
		if slot.status == occTxExecuted || slot.status == occTxValidated {
			task := occTask{kind: occTaskValidate, txIdx: idx, incarnation: slot.incarnation}
			slot.mu.Unlock()
			return task
		}
		slot.mu.Unlock()
	}
	s.numActive.Add(-1)
	return occTask{}
}

// nextTask returns the next unit of work, preferring validation when it lags
// execution, or none when nothing can be dispatched right now.
func (s *occScheduler) nextTask() occTask {
	for !s.isDone() {
		execIdx := s.executionIdx.Load()
		valIdx := s.validationIdx.Load()
		if valIdx >= int64(s.n) && execIdx >= int64(s.n) {
			return occTask{}
		}
		var task occTask
		if valIdx < execIdx {
			task = s.nextToValidate()
		} else {
			task = s.nextToExecute()
		}
		if task.kind != occTaskNone {
			return task
		}
	}
	return occTask{}
}

// addDependency records that txIdx read an estimate written by blocking. It
// returns false when blocking has already finished executing, in which case
// the caller should simply retry. Otherwise txIdx is parked until blocking's
// next execution completes; the caller's active task is released.
func (s *occScheduler) addDependency(txIdx, blocking int) bool {
	bslot := &s.txs[blocking]
	bslot.mu.Lock()
	if bslot.status == occTxExecuted || bslot.status == occTxValidated {
		bslot.mu.Unlock()
		return false
	}
	slot := &s.txs[txIdx]
	slot.mu.Lock()
	slot.status = occTxAborting
	slot.mu.Unlock()
	bslot.dependents = append(bslot.dependents, txIdx)
	bslot.mu.Unlock()
	s.numActive.Add(-1)
	return true
}

// finishExecution publishes an execution, wakes transactions that were waiting
// on it, and decides what to validate: if new locations were written every
// higher transaction must be re-validated, otherwise only this one.
func (s *occScheduler) finishExecution(txIdx, incarnation int, wroteNewLocation bool) occTask {
	slot := &s.txs[txIdx]
	slot.mu.Lock()
	slot.status = occTxExecuted
	deps := slot.dependents
	slot.dependents = nil
	slot.mu.Unlock()

	if len(deps) > 0 {
		minDep := -1
		for _, d := range deps {
			dslot := &s.txs[d]
			dslot.mu.Lock()
			dslot.status = occTxReady
			dslot.mu.Unlock()
			if minDep < 0 || d < minDep {
				minDep = d
			}
		}
		s.decreaseExecutionIdx(minDep)
	}

	if s.validationIdx.Load() > int64(txIdx) {
		if wroteNewLocation {
			s.decreaseValidationIdx(txIdx)
		} else {
			return occTask{kind: occTaskValidate, txIdx: txIdx, incarnation: incarnation}
		}
	}
	s.numActive.Add(-1)
	return occTask{}
}

// tryValidationAbort claims the right to abort an incarnation that failed
// validation. Exactly one of any concurrent failing validations wins.
func (s *occScheduler) tryValidationAbort(txIdx, incarnation int) bool {
	slot := &s.txs[txIdx]
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.incarnation != incarnation {
		return false
	}
	switch slot.status {
	case occTxValidated:
		s.numValidated.Add(-1)
	case occTxExecuted:
	default:
		return false
	}
	slot.status = occTxAborting
	return true
}

// finishValidation completes a validation task. An aborted transaction is
// scheduled for its next incarnation and every higher transaction is queued
// for re-validation; a passing one is marked validated.
func (s *occScheduler) finishValidation(txIdx, incarnation int, aborted bool) (occTask, error) {
	slot := &s.txs[txIdx]
	if aborted {
		slot.mu.Lock()
		next := slot.incarnation + 1
		if next >= occMaxTxIncarnations {
			slot.mu.Unlock()
			s.numActive.Add(-1)
			return occTask{}, errOCCMaxIncarnation
		}
		slot.incarnation = next
		slot.status = occTxReady
		slot.mu.Unlock()
		s.decreaseValidationIdx(txIdx + 1)
		if s.executionIdx.Load() > int64(txIdx) {
			// The active count is carried into the returned task, or released by
			// tryIncarnate when the claim fails.
			return s.tryIncarnate(txIdx), nil
		}
		s.numActive.Add(-1)
		return occTask{}, nil
	}
	slot.mu.Lock()
	if slot.incarnation == incarnation && slot.status == occTxExecuted {
		slot.status = occTxValidated
		s.numValidated.Add(1)
	}
	slot.mu.Unlock()
	s.numActive.Add(-1)
	return occTask{}, nil
}
