package evmonly

import (
	"hash/fnv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOCCSchedulerSingleWorkerSweep(t *testing.T) {
	s := newOCCScheduler(3)

	task := s.nextTask()
	require.Equal(t, occTask{kind: occTaskExecute, txIdx: 0}, task)
	// validationIdx (0) is not above 0: nothing to return, the sweep validates it.
	require.Equal(t, occTask{}, s.finishExecution(0, 0, true))

	task = s.nextTask()
	require.Equal(t, occTask{kind: occTaskValidate, txIdx: 0, wave: task.wave}, task, "validation is preferred once it lags execution")
	task, err := s.finishValidation(0, 0, false, 0)
	require.NoError(t, err)
	require.Equal(t, occTask{}, task)

	for idx := 1; idx < 3; idx++ {
		task = s.nextTask()
		require.Equal(t, occTask{kind: occTaskExecute, txIdx: idx}, task)
		require.Equal(t, occTask{}, s.finishExecution(idx, 0, true))
		task = s.nextTask()
		require.Equal(t, occTask{kind: occTaskValidate, txIdx: idx, wave: task.wave}, task)
		task, err = s.finishValidation(idx, 0, false, task.wave)
		require.NoError(t, err)
		require.Equal(t, occTask{}, task)
	}
	require.Equal(t, occTask{}, s.nextTask())
	require.True(t, s.isDone())
	require.True(t, s.allValidated())
	require.Zero(t, s.numActive.Load())
}

func TestOCCSchedulerValidationAbortReschedulesExecution(t *testing.T) {
	s := newOCCScheduler(2)
	require.Equal(t, occTask{kind: occTaskExecute, txIdx: 0}, s.nextTask())
	require.Equal(t, occTask{}, s.finishExecution(0, 0, true))
	task := s.nextTask()
	require.Equal(t, occTask{kind: occTaskValidate, txIdx: 0, wave: task.wave}, task)
	_, err := s.finishValidation(0, 0, false, task.wave)
	require.NoError(t, err)
	require.Equal(t, occTask{kind: occTaskExecute, txIdx: 1}, s.nextTask())
	require.Equal(t, occTask{}, s.finishExecution(1, 0, true))
	task = s.nextTask()
	require.Equal(t, occTask{kind: occTaskValidate, txIdx: 1, wave: task.wave}, task)

	require.True(t, s.tryValidationAbort(1, 0))
	require.False(t, s.tryValidationAbort(1, 0), "only one abort per incarnation")
	task, err = s.finishValidation(1, 0, true, 0)
	require.NoError(t, err)
	require.Equal(t, occTask{kind: occTaskExecute, txIdx: 1, incarnation: 1}, task, "execution passed the tx, so the re-execution is handed back")
	require.Equal(t, 2, s.validationIndex())
	require.False(t, s.isDone())

	// No new locations: only the transaction itself is re-validated.
	task = s.finishExecution(1, 1, false)
	require.Equal(t, occTask{kind: occTaskValidate, txIdx: 1, incarnation: 1, wave: task.wave}, task)
	require.False(t, s.tryValidationAbort(1, 0), "stale incarnation cannot abort")
	task, err = s.finishValidation(1, 1, false, 0)
	require.NoError(t, err)
	require.Equal(t, occTask{}, task)
	require.True(t, s.isDone())
	require.True(t, s.allValidated())
}

func TestOCCSchedulerAbortWithNewLocationsRewindsValidation(t *testing.T) {
	s := newOCCScheduler(3)
	for idx := range 3 {
		require.Equal(t, occTask{kind: occTaskExecute, txIdx: idx}, s.nextToExecute())
		require.Equal(t, occTask{}, s.finishExecution(idx, 0, true))
	}
	for idx := range 3 {
		task := s.nextTask()
		require.Equal(t, occTask{kind: occTaskValidate, txIdx: idx, wave: task.wave}, task)
		_, err := s.finishValidation(idx, 0, false, task.wave)
		require.NoError(t, err)
	}
	require.True(t, s.allValidated())

	// A lower re-execution rewinds validation; the re-validation of tx0 then
	// fails, rewinding validation to 1, and tx0's re-execution with new writes
	// rewinds it to 0.
	s.decreaseValidationIdx(0)
	require.Equal(t, occTask{kind: occTaskValidate, txIdx: 0, wave: 1}, s.nextTask(), "the rewind started wave 1")
	require.True(t, s.tryValidationAbort(0, 0))
	require.False(t, s.allValidated())
	task, err := s.finishValidation(0, 0, true, 0)
	require.NoError(t, err)
	require.Equal(t, occTask{kind: occTaskExecute, txIdx: 0, incarnation: 1}, task)
	require.Equal(t, 1, s.validationIndex())
	require.Equal(t, occTask{}, s.finishExecution(0, 1, true))
	require.Equal(t, 0, s.validationIndex())
	require.False(t, s.isDone())

	for idx := range 3 {
		task = s.nextTask()
		require.Equal(t, occTaskValidate, task.kind)
		require.Equal(t, idx, task.txIdx)
		_, err := s.finishValidation(task.txIdx, task.incarnation, false, task.wave)
		require.NoError(t, err)
	}
	require.True(t, s.isDone())
	require.True(t, s.allValidated())
}

func TestOCCSchedulerDependencies(t *testing.T) {
	s := newOCCScheduler(3)
	require.Equal(t, occTask{kind: occTaskExecute, txIdx: 0}, s.nextTask())
	require.Equal(t, occTask{kind: occTaskExecute, txIdx: 1}, s.nextTask(), "tx0 is executing so its validation is skipped")
	require.Equal(t, occTask{kind: occTaskExecute, txIdx: 2}, s.nextTask())
	require.Equal(t, int64(3), s.numActive.Load())

	require.True(t, s.addDependency(2, 0))
	require.Equal(t, int64(2), s.numActive.Load())
	require.Equal(t, occTxAborting, s.txs[2].status)

	require.Equal(t, occTask{}, s.finishExecution(0, 0, true))
	require.Equal(t, occTxReady, s.txs[2].status, "dependents are woken")
	require.Equal(t, int64(2), s.executionIdx.Load(), "execution rewinds to the woken dependent")

	require.False(t, s.addDependency(1, 0), "tx0 already executed: caller retries")
	require.Equal(t, occTxExecuting, s.txs[1].status)
	require.Equal(t, occTask{}, s.finishExecution(1, 0, false))

	task := s.nextTask()
	require.Equal(t, occTask{kind: occTaskValidate, txIdx: 0, wave: task.wave}, task)
	_, err := s.finishValidation(0, 0, false, task.wave)
	require.NoError(t, err)
	task = s.nextTask()
	require.Equal(t, occTask{kind: occTaskValidate, txIdx: 1, wave: task.wave}, task)
	_, err = s.finishValidation(1, 0, false, task.wave)
	require.NoError(t, err)
	require.Equal(t, occTask{kind: occTaskExecute, txIdx: 2}, s.nextTask())
	require.Equal(t, occTask{}, s.finishExecution(2, 0, true))
	task = s.nextTask()
	require.Equal(t, occTask{kind: occTaskValidate, txIdx: 2, wave: task.wave}, task)
	_, err = s.finishValidation(2, 0, false, task.wave)
	require.NoError(t, err)
	require.True(t, s.isDone())
	require.True(t, s.allValidated())
}

func TestOCCSchedulerIncarnationCap(t *testing.T) {
	s := newOCCScheduler(1)
	require.Equal(t, occTask{kind: occTaskExecute, txIdx: 0}, s.nextTask())
	s.txs[0].incarnation = occMaxTxIncarnations - 2
	require.Equal(t, occTask{}, s.finishExecution(0, occMaxTxIncarnations-2, true))
	task := s.nextTask()
	require.Equal(t, occTaskValidate, task.kind)
	require.True(t, s.tryValidationAbort(0, occMaxTxIncarnations-2))
	task, err := s.finishValidation(0, occMaxTxIncarnations-2, true, 0)
	require.NoError(t, err)
	require.Equal(t, occMaxTxIncarnations-1, task.incarnation)
	require.Equal(t, occTask{}, s.finishExecution(0, occMaxTxIncarnations-1, true))
	task = s.nextTask()
	require.True(t, s.tryValidationAbort(0, occMaxTxIncarnations-1))
	_, err = s.finishValidation(0, occMaxTxIncarnations-1, true, 0)
	require.ErrorIs(t, err, errOCCMaxIncarnation)
}

// TestOCCSchedulerConcurrentTermination drives the scheduler from several
// goroutines with a fake executor whose validations fail on a fixed pattern.
// It must terminate with every transaction validated.
func TestOCCSchedulerConcurrentTermination(t *testing.T) {
	const n, workers = 96, 8
	fails := func(idx, inc int) bool {
		if inc >= 3 {
			return false
		}
		h := fnv.New32a()
		h.Write([]byte{byte(idx), byte(idx >> 8), byte(inc)})
		return h.Sum32()%3 == 0
	}
	for round := 0; round < 20; round++ {
		s := newOCCScheduler(n)
		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				var task occTask
				for !s.isDone() {
					if task.kind == occTaskNone {
						task = s.nextTask()
						continue
					}
					switch task.kind {
					case occTaskExecute:
						// Every other transaction pretends to hit an estimate of its
						// predecessor on its first incarnation.
						if task.incarnation == 0 && task.txIdx%2 == 1 && s.addDependency(task.txIdx, task.txIdx-1) {
							task = occTask{}
							continue
						}
						task = s.finishExecution(task.txIdx, task.incarnation, task.incarnation == 0)
					case occTaskValidate:
						aborted := fails(task.txIdx, task.incarnation) && s.tryValidationAbort(task.txIdx, task.incarnation)
						var err error
						task, err = s.finishValidation(task.txIdx, task.incarnation, aborted, task.wave)
						require.NoError(t, err)
					}
				}
			}()
		}
		wg.Wait()
		require.True(t, s.allValidated(), "round %d", round)
		require.Zero(t, s.numActive.Load())
		for i := range s.txs {
			require.Equal(t, occTxValidated, s.txs[i].status, "tx %d", i)
		}
	}
}

// TestOCCSchedulerCommitCursorWaves: a transaction validated before a lower
// transaction was re-executed is not final until it is validated again in the
// wave that re-execution started.
func TestOCCSchedulerCommitCursorWaves(t *testing.T) {
	s := newOCCScheduler(2)
	var committed []int
	commit := func(idx int) { committed = append(committed, idx) }

	require.Equal(t, occTask{kind: occTaskExecute, txIdx: 0}, s.nextTask())
	require.Equal(t, occTask{kind: occTaskExecute, txIdx: 1}, s.nextTask())
	require.Equal(t, occTask{}, s.finishExecution(1, 0, true))
	require.Equal(t, occTask{}, s.nextTask(), "validation waits for tx0, which is still executing")
	require.Equal(t, occTask{}, s.finishExecution(0, 0, true))
	for idx := range 2 {
		task := s.nextTask()
		require.Equal(t, occTask{kind: occTaskValidate, txIdx: idx, wave: 0}, task, "no rewind happened: wave 0")
		_, err := s.finishValidation(idx, 0, false, task.wave)
		require.NoError(t, err)
	}

	// tx0 is invalidated by a late re-validation and re-executes: tx1's wave-0
	// validation predates tx0's new writes.
	s.decreaseValidationIdx(0)
	task := s.nextTask()
	require.Equal(t, occTask{kind: occTaskValidate, txIdx: 0, wave: 1}, task)
	require.True(t, s.tryValidationAbort(0, 0))
	task, err := s.finishValidation(0, 0, true, task.wave)
	require.NoError(t, err)
	require.Equal(t, occTask{kind: occTaskExecute, txIdx: 0, incarnation: 1}, task)
	s.tryCommit(commit)
	require.Empty(t, committed, "tx0 is being re-executed")
	require.Equal(t, occTask{}, s.nextTask(), "tx1 is not re-validated while tx0 executes")

	require.Equal(t, occTask{}, s.finishExecution(0, 1, true))
	task = s.nextTask()
	require.Equal(t, occTask{kind: occTaskValidate, txIdx: 0, incarnation: 1, wave: 2}, task)
	_, err = s.finishValidation(0, 1, false, task.wave)
	require.NoError(t, err)
	s.tryCommit(commit)
	require.Equal(t, []int{0}, committed, "tx0 is final; tx1's wave-0 validation is stale")
	require.True(t, s.isCommitted(0))
	require.False(t, s.isCommitted(1))

	task = s.nextTask()
	require.Equal(t, occTask{kind: occTaskValidate, txIdx: 1, wave: 2}, task)
	_, err = s.finishValidation(1, 0, false, task.wave)
	require.NoError(t, err)
	s.tryCommit(commit)
	require.Equal(t, []int{0, 1}, committed)
	require.False(t, s.tryValidationAbort(1, 0), "a committed transaction cannot be aborted")
	require.True(t, s.isDone())
	require.True(t, s.allValidated())
}

// TestOCCSchedulerValidationWaitsForExecutedFrontier: with two workers, the
// higher transaction finishing first is not validated until the lower one
// has executed, so a conflict-free block validates each transaction once.
func TestOCCSchedulerValidationWaitsForExecutedFrontier(t *testing.T) {
	s := newOCCScheduler(3)
	require.Equal(t, occTask{kind: occTaskExecute, txIdx: 0}, s.nextTask())
	require.Equal(t, occTask{kind: occTaskExecute, txIdx: 1}, s.nextTask())
	require.Equal(t, occTask{}, s.finishExecution(1, 0, true))
	require.Equal(t, occTask{kind: occTaskExecute, txIdx: 2}, s.nextTask(), "execution continues while validation waits")
	require.Equal(t, occTask{}, s.finishExecution(2, 0, true))
	require.Equal(t, occTask{}, s.nextTask())
	require.False(t, s.isDone())
	require.Equal(t, occTask{}, s.finishExecution(0, 0, true))
	validations := 0
	for {
		task := s.nextTask()
		if task.kind == occTaskNone {
			break
		}
		require.Equal(t, occTaskValidate, task.kind)
		validations++
		_, err := s.finishValidation(task.txIdx, task.incarnation, false, task.wave)
		require.NoError(t, err)
	}
	require.Equal(t, 3, validations)
	require.True(t, s.isDone())
	require.True(t, s.allValidated())
}
