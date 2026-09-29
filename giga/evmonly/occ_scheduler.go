package evmonly

import (
	"errors"
	"sync"
	"sync/atomic"
)

// The Block-STM collaborative scheduler (Gelashvili et al., Algorithms 3-5)
// with the VALIDATED status pevm added for bookkeeping and the commit cursor of
// the Aptos implementation. Two shared counters hand out work: executionIdx is
// the next transaction to execute, validationIdx the next to validate.
// Validation is preferred whenever it lags execution. Aborts rewind the
// counters; the block is done when both counters have passed the end, no task
// is in flight, and no rewind happened while that was observed.
//
// Every rewind of validationIdx starts a new validation wave. A transaction is
// final once it passed validation in a wave at least as new as every rewind at
// or below its index; the commit cursor advances over final transactions in
// order and lets multi-version memory materialise their writes.
//
// Validation never runs ahead of the executed frontier, the lowest index whose
// current incarnation has not finished executing. A validation of a
// transaction with an unfinished lower transaction would be repeated once that
// transaction publishes, so it is not dispatched until then; in a block without
// conflicts every transaction is validated exactly once.

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
	wave        int64 // validation tasks: the wave the task was dispatched in
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
	// validatedWave is the newest wave in which the current incarnation passed
	// validation; requiredWave the wave its validation must be at least, set
	// when it was aborted; maxTriggeredWave the newest wave started by a rewind
	// that targeted this index.
	validatedWave    int64
	requiredWave     int64
	maxTriggeredWave int64
}

var errOCCSchedulerInvariant = errors.New("occ scheduler finished with unvalidated transactions")

type occScheduler struct {
	n            int
	executionIdx atomic.Int64
	// validationIdx packs the next index to validate in its low 32 bits and the
	// current wave in its high 32 bits, so a rewind and its wave change are one
	// atomic step.
	validationIdx atomic.Int64
	numActive     atomic.Int64 // dispatched tasks not yet finished
	decreaseCnt   atomic.Int64 // bumped on every counter rewind
	numValidated  atomic.Int64
	done          atomic.Bool
	txs           []occTxSlot
	// executedFrontier is the lowest index whose current incarnation has not
	// finished executing (n when all have). It advances in finishExecution and
	// retreats when a transaction is aborted.
	executedFrontier atomic.Int64
	// commitIdx is the next transaction to commit; commitMu serialises the
	// cursor's advance. commitWave is the newest wave of any rewind that
	// targeted a committed index; it is written under commitMu and read
	// without it only for the advisory re-check after releasing the lock.
	commitIdx  atomic.Int64
	commitMu   sync.Mutex
	commitWave atomic.Int64
}

func newOCCScheduler(n int) *occScheduler {
	return &occScheduler{n: n, txs: make([]occTxSlot, n)}
}

func packValidationIdx(idx int, wave int64) int64 {
	return wave<<32 | int64(idx)
}

func unpackValidationIdx(v int64) (int, int64) {
	return int(v & 0xffffffff), v >> 32
}

// validationIndex returns the next index to validate.
func (s *occScheduler) validationIndex() int {
	idx, _ := unpackValidationIdx(s.validationIdx.Load())
	return idx
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

// decreaseValidationIdx rewinds validation to target, starting a new wave when
// it actually moves the index, and returns the wave validations of target and
// above now run in.
func (s *occScheduler) decreaseValidationIdx(target int) int64 {
	for {
		cur := s.validationIdx.Load()
		idx, wave := unpackValidationIdx(cur)
		if idx <= target {
			s.decreaseCnt.Add(1)
			return wave
		}
		next := wave + 1
		if s.validationIdx.CompareAndSwap(cur, packValidationIdx(target, next)) {
			if target < s.n {
				slot := &s.txs[target]
				slot.mu.Lock()
				if next > slot.maxTriggeredWave {
					slot.maxTriggeredWave = next
				}
				slot.mu.Unlock()
			}
			s.decreaseCnt.Add(1)
			return next
		}
	}
}

// isDone applies the paper's completion rule. The rewind counter is read
// before and after the index and active-task checks so a rewind that races
// with the observation forces another round.
func (s *occScheduler) isDone() bool {
	if s.done.Load() {
		return true
	}
	observed := s.decreaseCnt.Load()
	if min(int(s.executionIdx.Load()), s.validationIndex()) >= s.n &&
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

// advanceExecutedFrontier moves the frontier over transactions that have
// finished their current incarnation.
func (s *occScheduler) advanceExecutedFrontier() {
	for {
		f := int(s.executedFrontier.Load())
		if f >= s.n {
			return
		}
		slot := &s.txs[f]
		slot.mu.Lock()
		executed := slot.status == occTxExecuted || slot.status == occTxValidated
		slot.mu.Unlock()
		if !executed || !s.executedFrontier.CompareAndSwap(int64(f), int64(f+1)) {
			if !executed {
				return
			}
		}
	}
}

func (s *occScheduler) nextToValidate() occTask {
	if idx := s.validationIndex(); idx >= s.n || int64(idx) >= s.executedFrontier.Load() {
		return occTask{}
	}
	s.numActive.Add(1)
	idx, wave := unpackValidationIdx(s.validationIdx.Add(1) - 1)
	if idx < s.n {
		slot := &s.txs[idx]
		slot.mu.Lock()
		if slot.status == occTxExecuted || slot.status == occTxValidated {
			task := occTask{kind: occTaskValidate, txIdx: idx, incarnation: slot.incarnation, wave: wave}
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
		execIdx := int(s.executionIdx.Load())
		valIdx := s.validationIndex()
		if valIdx >= s.n && execIdx >= s.n {
			return occTask{}
		}
		var task occTask
		if valIdx < execIdx && int64(valIdx) < s.executedFrontier.Load() {
			task = s.nextToValidate()
		}
		if task.kind == occTaskNone {
			task = s.nextToExecute()
		}
		if task.kind != occTaskNone {
			return task
		}
		if execIdx >= s.n {
			// Everything is dispatched and validation waits for an execution in
			// flight: nothing to hand out until it finishes.
			return occTask{}
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
	s.advanceExecutedFrontier()

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

	if valIdx, wave := unpackValidationIdx(s.validationIdx.Load()); valIdx > txIdx {
		if wroteNewLocation {
			s.decreaseValidationIdx(txIdx)
		} else {
			return occTask{kind: occTaskValidate, txIdx: txIdx, incarnation: incarnation, wave: wave}
		}
	}
	s.numActive.Add(-1)
	return occTask{}
}

// tryValidationAbort claims the right to abort an incarnation that failed
// validation. Exactly one of any concurrent failing validations wins. A
// committed transaction can no longer be aborted.
func (s *occScheduler) tryValidationAbort(txIdx, incarnation int) bool {
	if int64(txIdx) < s.commitIdx.Load() {
		return false
	}
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
	fetchMin(&s.executedFrontier, int64(txIdx))
	return true
}

// finishValidation completes a validation task dispatched in wave. An aborted
// transaction is scheduled for its next incarnation and every higher
// transaction is queued for re-validation; a passing one is marked validated
// in that wave.
func (s *occScheduler) finishValidation(txIdx, incarnation int, aborted bool, wave int64) (occTask, error) {
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
		// Locks txIdx then txIdx+1: ascending, as everywhere else.
		slot.requiredWave = s.decreaseValidationIdx(txIdx + 1)
		slot.mu.Unlock()
		if s.executionIdx.Load() > int64(txIdx) {
			// The active count is carried into the returned task, or released by
			// tryIncarnate when the claim fails.
			return s.tryIncarnate(txIdx), nil
		}
		s.numActive.Add(-1)
		return occTask{}, nil
	}
	slot.mu.Lock()
	if slot.incarnation == incarnation {
		if slot.status == occTxExecuted {
			slot.status = occTxValidated
			s.numValidated.Add(1)
		}
		if slot.status == occTxValidated && wave > slot.validatedWave {
			slot.validatedWave = wave
		}
	}
	slot.mu.Unlock()
	s.numActive.Add(-1)
	return occTask{}, nil
}

// isCommitted reports whether the commit cursor has passed txIdx.
func (s *occScheduler) isCommitted(txIdx int) bool {
	return int64(txIdx) < s.commitIdx.Load()
}

// tryCommit advances the commit cursor over every transaction that is final:
// validated in a wave at least as new as every rewind at or below its index.
// commit runs once per committed transaction, in order, before the cursor
// passes it. Callers that find the cursor busy leave; the holder re-checks
// after releasing it so a transaction validated meanwhile is not left behind.
func (s *occScheduler) tryCommit(commit func(txIdx int)) {
	for {
		if !s.commitMu.TryLock() {
			return
		}
		for {
			idx := int(s.commitIdx.Load())
			if idx >= s.n {
				break
			}
			slot := &s.txs[idx]
			slot.mu.Lock()
			wave := max(s.commitWave.Load(), slot.maxTriggeredWave)
			s.commitWave.Store(wave)
			final := slot.status == occTxValidated && slot.validatedWave >= max(wave, slot.requiredWave)
			slot.mu.Unlock()
			if !final {
				break
			}
			commit(idx)
			s.commitIdx.Store(int64(idx + 1))
		}
		s.commitMu.Unlock()
		idx := int(s.commitIdx.Load())
		if idx >= s.n {
			return
		}
		slot := &s.txs[idx]
		slot.mu.Lock()
		again := slot.status == occTxValidated && slot.validatedWave >= max(s.commitWave.Load(), slot.maxTriggeredWave, slot.requiredWave)
		slot.mu.Unlock()
		if !again {
			return
		}
	}
}
