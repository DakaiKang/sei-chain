package evmonly

import (
	"encoding/binary"
	"math/big"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
)

// txVersion identifies one incarnation of one transaction.
type txVersion struct {
	txIdx       int32
	incarnation int32
}

type mvEntryKind uint8

const (
	// mvValue is a post-value written by an incarnation.
	mvValue mvEntryKind = iota
	// mvDelta is a commutative balance credit; readers fold every delta above
	// the nearest lower value.
	mvDelta
	// mvEstimate marks a location an aborted incarnation wrote. Readers that
	// need it abort and wait for the writer's next incarnation.
	mvEstimate
)

// mvEntry is one version of one location. Which payload field is meaningful
// depends on the location kind.
type mvEntry struct {
	version txVersion
	kind    mvEntryKind
	u256    uint256.Int // balance value, delta amount, or storage slot (big-endian)
	nonce   uint64
	code    []byte // immutable once stored; nil means no code
}

// mvLocation is the version list of one location, ascending by txIdx and
// unique per txIdx: a new incarnation replaces its predecessor's entry.
type mvLocation struct {
	mu      sync.RWMutex
	entries []mvEntry
}

// lowerBound returns the number of entries with txIdx < idx.
func (l *mvLocation) lowerBound(idx int) int {
	return sort.Search(len(l.entries), func(i int) bool { return int(l.entries[i].version.txIdx) >= idx })
}

func (l *mvLocation) find(idx int) (int, bool) {
	i := l.lowerBound(idx)
	return i, i < len(l.entries) && int(l.entries[i].version.txIdx) == idx
}

func (l *mvLocation) upsert(e mvEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	i, ok := l.find(int(e.version.txIdx))
	if ok {
		l.entries[i] = e
		return
	}
	l.entries = append(l.entries, mvEntry{})
	copy(l.entries[i+1:], l.entries[i:])
	l.entries[i] = e
}

func (l *mvLocation) remove(idx int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	i, ok := l.find(idx)
	if !ok {
		return
	}
	copy(l.entries[i:], l.entries[i+1:])
	l.entries[len(l.entries)-1] = mvEntry{}
	l.entries = l.entries[:len(l.entries)-1]
}

func (l *mvLocation) setEstimate(idx int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if i, ok := l.find(idx); ok {
		l.entries[i].kind = mvEstimate
		l.entries[i].code = nil
	}
}

// latestBelow returns a copy of the highest entry with txIdx < idx.
func (l *mvLocation) latestBelow(idx int) (mvEntry, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	i := l.lowerBound(idx)
	if i == 0 {
		return mvEntry{}, false
	}
	return l.entries[i-1], true
}

const mvShardCount = 256

type mvShard struct {
	mu   sync.RWMutex
	locs map[stateAccessKey]*mvLocation
	// pool holds this shard's released locations for reuse in later blocks. It
	// is only touched under mu, so each shard recycles its own locations.
	pool []*mvLocation
}

func mvShardIndex(k stateAccessKey) int {
	h := binary.LittleEndian.Uint64(k.address[12:20]) ^ binary.LittleEndian.Uint64(k.slot[24:32])
	h ^= uint64(k.kind) * 0x9E3779B97F4A7C15
	h ^= h >> 29
	return int(h % mvShardCount)
}

// mvTxState is the last recorded incarnation of one transaction: what it wrote
// (for removal and estimate conversion) and what it read (for validation).
type mvTxState struct {
	mu        sync.Mutex
	version   txVersion
	recorded  bool
	writeKeys []stateAccessKey
	reads     []mvRead
}

// mvMemory is the Block-STM multi-version memory for one block.
type mvMemory struct {
	snapshot  StateReader
	shards    [mvShardCount]mvShard
	txs       []mvTxState
	hasClears atomic.Bool
}

func newMVMemory() *mvMemory {
	m := &mvMemory{}
	for i := range m.shards {
		m.shards[i].locs = map[stateAccessKey]*mvLocation{}
	}
	return m
}

// reset prepares the memory for a new block. Not safe for concurrent use.
func (m *mvMemory) reset(snapshot StateReader, txCount int) {
	if snapshot == nil {
		snapshot = NewMemoryState()
	}
	m.snapshot = snapshot
	for i := range m.shards {
		shard := &m.shards[i]
		for _, loc := range shard.locs {
			for j := range loc.entries {
				loc.entries[j] = mvEntry{}
			}
			loc.entries = loc.entries[:0]
			shard.pool = append(shard.pool, loc)
		}
		clear(shard.locs)
	}
	if cap(m.txs) >= txCount {
		m.txs = m.txs[:txCount]
	} else {
		m.txs = make([]mvTxState, txCount)
	}
	for i := range m.txs {
		m.txs[i] = mvTxState{}
	}
	m.hasClears.Store(false)
}

func (m *mvMemory) lookup(k stateAccessKey) *mvLocation {
	shard := &m.shards[mvShardIndex(k)]
	shard.mu.RLock()
	loc := shard.locs[k]
	shard.mu.RUnlock()
	return loc
}

func (m *mvMemory) lookupOrCreate(k stateAccessKey) *mvLocation {
	shard := &m.shards[mvShardIndex(k)]
	shard.mu.RLock()
	loc := shard.locs[k]
	shard.mu.RUnlock()
	if loc != nil {
		return loc
	}
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if loc = shard.locs[k]; loc != nil {
		return loc
	}
	if n := len(shard.pool); n > 0 {
		loc = shard.pool[n-1]
		shard.pool[n-1] = nil
		shard.pool = shard.pool[:n-1]
	} else {
		loc = &mvLocation{}
	}
	shard.locs[k] = loc
	return loc
}

func (m *mvMemory) latestBelow(k stateAccessKey, txIdx int) (mvEntry, bool) {
	loc := m.lookup(k)
	if loc == nil {
		return mvEntry{}, false
	}
	return loc.latestBelow(txIdx)
}

// readOriginKind says where a read was served from.
type readOriginKind uint8

const (
	// originSnapshot: no lower-index write existed; the block snapshot answered.
	originSnapshot readOriginKind = iota
	// originVersion: a lower-index value entry answered.
	originVersion
	// originClear: a lower-index storage clear made the slot zero.
	originClear
)

type readOrigin struct {
	kind    readOriginKind
	version txVersion
}

// mvRead is one validated read: the key, where it resolved, the delta versions
// folded on top (balances only), and the value that was returned.
type mvRead struct {
	key    stateAccessKey
	origin readOrigin
	fold   []txVersion
	u256   uint256.Int
	nonce  uint64
	code   []byte
}

// mvResolution is the outcome of resolving one key at one index.
type mvResolution struct {
	origin   readOrigin
	fold     []txVersion
	blocking int // >= 0 when an estimate was hit
	overflow bool
}

func (m *mvMemory) resolveBalance(addr common.Address, txIdx int, foldBuf []txVersion) (uint256.Int, mvResolution) {
	res := mvResolution{blocking: -1}
	var sum uint256.Int
	var base uint256.Int
	found := false
	if loc := m.lookup(stateAccessKey{kind: stateAccessBalance, address: addr}); loc != nil {
		loc.mu.RLock()
		for i := loc.lowerBound(txIdx) - 1; i >= 0; i-- {
			e := &loc.entries[i]
			switch e.kind {
			case mvEstimate:
				res.blocking = int(e.version.txIdx)
			case mvDelta:
				foldBuf = append(foldBuf, e.version)
				if _, carry := sum.AddOverflow(&sum, &e.u256); carry {
					res.overflow = true
				}
				continue
			case mvValue:
				base = e.u256
				res.origin = readOrigin{kind: originVersion, version: e.version}
				found = true
			}
			break
		}
		loc.mu.RUnlock()
	}
	res.fold = foldBuf
	if res.blocking >= 0 {
		return uint256.Int{}, res
	}
	if !found {
		b, err := uint256FromBig(m.snapshot.GetBalance(addr))
		if err != nil {
			res.overflow = true
		} else {
			base = *b
		}
		res.origin = readOrigin{kind: originSnapshot}
	}
	var out uint256.Int
	if _, carry := out.AddOverflow(&base, &sum); carry {
		res.overflow = true
	}
	return out, res
}

func (m *mvMemory) resolveNonce(addr common.Address, txIdx int) (uint64, mvResolution) {
	res := mvResolution{blocking: -1}
	if e, ok := m.latestBelow(stateAccessKey{kind: stateAccessNonce, address: addr}, txIdx); ok {
		if e.kind == mvEstimate {
			res.blocking = int(e.version.txIdx)
			return 0, res
		}
		res.origin = readOrigin{kind: originVersion, version: e.version}
		return e.nonce, res
	}
	res.origin = readOrigin{kind: originSnapshot}
	return m.snapshot.GetNonce(addr), res
}

func (m *mvMemory) resolveCode(addr common.Address, txIdx int) ([]byte, mvResolution) {
	res := mvResolution{blocking: -1}
	if e, ok := m.latestBelow(stateAccessKey{kind: stateAccessCode, address: addr}, txIdx); ok {
		if e.kind == mvEstimate {
			res.blocking = int(e.version.txIdx)
			return nil, res
		}
		res.origin = readOrigin{kind: originVersion, version: e.version}
		return e.code, res
	}
	res.origin = readOrigin{kind: originSnapshot}
	return m.snapshot.GetCode(addr), res
}

func (m *mvMemory) resolveStorage(addr common.Address, slot common.Hash, txIdx int) (common.Hash, mvResolution) {
	res := mvResolution{blocking: -1}
	w, haveW := m.latestBelow(stateAccessKey{kind: stateAccessStorage, address: addr, slot: slot}, txIdx)
	var c mvEntry
	haveC := false
	if m.hasClears.Load() {
		c, haveC = m.latestBelow(stateAccessKey{kind: stateAccessStorageClear, address: addr}, txIdx)
	}
	// A clear strictly newer than the latest slot write zeroes the slot. A clear
	// and a write at the same index come from one incarnation that cleared and
	// then wrote, so the write wins.
	if haveC && (!haveW || c.version.txIdx > w.version.txIdx) {
		if c.kind == mvEstimate {
			res.blocking = int(c.version.txIdx)
			return common.Hash{}, res
		}
		res.origin = readOrigin{kind: originClear, version: c.version}
		return common.Hash{}, res
	}
	if haveW {
		if w.kind == mvEstimate {
			res.blocking = int(w.version.txIdx)
			return common.Hash{}, res
		}
		res.origin = readOrigin{kind: originVersion, version: w.version}
		return w.u256.Bytes32(), res
	}
	res.origin = readOrigin{kind: originSnapshot}
	return m.snapshot.GetState(addr, slot), res
}

// record installs the incarnation's writes, removes locations the previous
// incarnation wrote but this one did not, stores the read set for validation,
// and reports whether any location was written for the first time by this
// transaction.
func (m *mvMemory) record(txIdx, incarnation int, exec *occTxExecution) bool {
	v := txVersion{txIdx: int32(txIdx), incarnation: int32(incarnation)} //nolint:gosec // bounded by block size and occMaxTxIncarnations.
	st := &m.txs[txIdx]
	newKeys := make([]stateAccessKey, 0, len(exec.changeSet.Balances)+len(exec.changeSet.Nonces)+len(exec.changeSet.Code)+len(exec.changeSet.StorageClears)+len(exec.changeSet.Storage))
	if exec.err == nil {
		for _, ch := range exec.changeSet.Balances {
			k := stateAccessKey{kind: stateAccessBalance, address: ch.Address}
			e := mvEntry{version: v, kind: mvValue}
			delta := exec.commutativeBalanceDeltas[ch.Address]
			_, normal := exec.writeSet[k]
			if delta != nil && !normal {
				e.kind = mvDelta
				e.u256 = uint256FromBigOrMax(delta)
			} else {
				e.u256 = uint256FromBigOrMax(ch.Balance)
			}
			m.lookupOrCreate(k).upsert(e)
			newKeys = append(newKeys, k)
		}
		for _, ch := range exec.changeSet.Nonces {
			k := stateAccessKey{kind: stateAccessNonce, address: ch.Address}
			m.lookupOrCreate(k).upsert(mvEntry{version: v, kind: mvValue, nonce: ch.Nonce})
			newKeys = append(newKeys, k)
		}
		for _, ch := range exec.changeSet.Code {
			k := stateAccessKey{kind: stateAccessCode, address: ch.Address}
			e := mvEntry{version: v, kind: mvValue}
			if !ch.Delete && len(ch.Code) > 0 {
				e.code = ch.Code
			}
			m.lookupOrCreate(k).upsert(e)
			newKeys = append(newKeys, k)
		}
		if len(exec.changeSet.StorageClears) > 0 {
			m.hasClears.Store(true)
		}
		for _, addr := range exec.changeSet.StorageClears {
			k := stateAccessKey{kind: stateAccessStorageClear, address: addr}
			m.lookupOrCreate(k).upsert(mvEntry{version: v, kind: mvValue})
			newKeys = append(newKeys, k)
		}
		for _, ch := range exec.changeSet.Storage {
			k := stateAccessKey{kind: stateAccessStorage, address: ch.Address, slot: ch.Key}
			e := mvEntry{version: v, kind: mvValue}
			e.u256.SetBytes32(ch.Value[:])
			m.lookupOrCreate(k).upsert(e)
			newKeys = append(newKeys, k)
		}
	}

	st.mu.Lock()
	oldKeys := st.writeKeys
	st.writeKeys = newKeys
	st.reads = exec.reads
	st.version = v
	st.recorded = true
	st.mu.Unlock()

	wroteNew := false
	if len(oldKeys) == 0 {
		wroteNew = len(newKeys) > 0
	} else {
		old := make(map[stateAccessKey]struct{}, len(oldKeys))
		for _, k := range oldKeys {
			old[k] = struct{}{}
		}
		for _, k := range newKeys {
			if _, ok := old[k]; ok {
				delete(old, k)
			} else {
				wroteNew = true
			}
		}
		for k := range old {
			if loc := m.lookup(k); loc != nil {
				loc.remove(txIdx)
			}
		}
	}
	return wroteNew
}

// convertWritesToEstimates marks every location the transaction last wrote as
// an estimate so higher readers wait for its next incarnation.
func (m *mvMemory) convertWritesToEstimates(txIdx int) {
	st := &m.txs[txIdx]
	st.mu.Lock()
	keys := st.writeKeys
	st.mu.Unlock()
	for _, k := range keys {
		if loc := m.lookup(k); loc != nil {
			loc.setEstimate(txIdx)
		}
	}
}

// readsOf returns the last recorded read set of a transaction. The slice is
// replaced, never mutated, by record, so callers may iterate without the lock.
func (m *mvMemory) readsOf(txIdx int) ([]mvRead, txVersion, bool) {
	st := &m.txs[txIdx]
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.reads, st.version, st.recorded
}

func originsEqual(a, b readOrigin) bool {
	return a.kind == b.kind && (a.kind == originSnapshot || a.version == b.version)
}

func foldsEqual(a, b []txVersion) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// validateReadSet re-resolves every recorded read of a transaction at its
// index. In version mode a read is valid if it resolves to the same origin
// and delta fold; in value mode it is valid if it yields the same value. An
// estimate in the path always invalidates. The first few stale keys are
// returned for conflict samples along with the total count.
func (m *mvMemory) validateReadSet(txIdx int, valueBased bool) (bool, []stateAccessKey, int) {
	reads, _, recorded := m.readsOf(txIdx)
	if !recorded {
		return false, nil, 0
	}
	var stale []stateAccessKey
	staleCount := 0
	var foldBuf []txVersion
	for i := range reads {
		r := &reads[i]
		ok := true
		switch r.key.kind {
		case stateAccessBalance:
			var val uint256.Int
			var res mvResolution
			val, res = m.resolveBalance(r.key.address, txIdx, foldBuf[:0])
			foldBuf = res.fold
			switch {
			case res.blocking >= 0 || res.overflow:
				ok = false
			case valueBased:
				ok = val.Eq(&r.u256)
			default:
				ok = originsEqual(res.origin, r.origin) && foldsEqual(res.fold, r.fold)
			}
		case stateAccessNonce:
			val, res := m.resolveNonce(r.key.address, txIdx)
			switch {
			case res.blocking >= 0:
				ok = false
			case valueBased:
				ok = val == r.nonce
			default:
				ok = originsEqual(res.origin, r.origin)
			}
		case stateAccessCode:
			val, res := m.resolveCode(r.key.address, txIdx)
			switch {
			case res.blocking >= 0:
				ok = false
			case valueBased:
				ok = bytesEqualNilSafe(val, r.code)
			default:
				ok = originsEqual(res.origin, r.origin)
			}
		case stateAccessStorage:
			val, res := m.resolveStorage(r.key.address, r.key.slot, txIdx)
			switch {
			case res.blocking >= 0:
				ok = false
			case valueBased:
				ok = val == r.u256.Bytes32()
			default:
				ok = originsEqual(res.origin, r.origin)
			}
		}
		if !ok {
			staleCount++
			if len(stale) < 8 {
				stale = append(stale, r.key)
			}
		}
	}
	return staleCount == 0, stale, staleCount
}

func bytesEqualNilSafe(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ChangeSetInto consolidates the latest version of every location into the
// block changeset, using the same emission rules as blockSTMState so both
// engines produce byte-identical output. Single-threaded; no estimates may
// remain.
func (m *mvMemory) ChangeSetInto(changes *StateChangeSet) {
	final := newBlockSTMState(m.snapshot)
	type slotFinal struct {
		writeIdx int32
		value    common.Hash
	}
	clearIdx := map[common.Address]int32{}
	slots := map[storageChangeKey]slotFinal{}
	for i := range m.shards {
		shard := &m.shards[i]
		for k, loc := range shard.locs {
			if len(loc.entries) == 0 {
				continue
			}
			top := loc.entries[len(loc.entries)-1]
			if top.kind == mvEstimate {
				panic("mv memory: estimate left after block completion")
			}
			switch k.kind {
			case stateAccessBalance:
				var sum uint256.Int
				var base *uint256.Int
				for j := len(loc.entries) - 1; j >= 0; j-- {
					e := &loc.entries[j]
					if e.kind == mvDelta {
						sum.Add(&sum, &e.u256)
						continue
					}
					if e.kind == mvValue {
						base = &e.u256
					}
					break
				}
				var out uint256.Int
				if base == nil {
					b, err := uint256FromBig(m.snapshot.GetBalance(k.address))
					if err == nil {
						out.Add(b, &sum)
					}
				} else {
					out.Add(base, &sum)
				}
				final.balances[k.address] = out.ToBig()
			case stateAccessNonce:
				final.nonces[k.address] = top.nonce
			case stateAccessCode:
				final.code[k.address] = cloneBytes(top.code)
			case stateAccessStorageClear:
				clearIdx[k.address] = top.version.txIdx
				final.storageClears[k.address] = struct{}{}
			case stateAccessStorage:
				slots[storageChangeKey{address: k.address, key: k.slot}] = slotFinal{writeIdx: top.version.txIdx, value: top.u256.Bytes32()}
			}
		}
	}
	for key, sf := range slots {
		if c, cleared := clearIdx[key.address]; cleared && c > sf.writeIdx {
			continue
		}
		final.storage[key] = sf.value
	}
	final.ChangeSetInto(changes)
}

func uint256FromBigOrMax(b *big.Int) uint256.Int {
	if b == nil || b.Sign() <= 0 {
		return uint256.Int{}
	}
	v, overflow := uint256.FromBig(b)
	if overflow {
		return *new(uint256.Int).SetAllOne()
	}
	return *v
}
