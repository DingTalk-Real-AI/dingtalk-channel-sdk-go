package safety

import (
	"sync"
	"time"

	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/types"
)

// Deduper 双层去重：协议层 messageId + 业务层 msgId，TTL + LRU + 后台清理（SPEC §3.2 / E6）。
type Deduper struct {
	cfg       types.DedupConfig
	mu        sync.Mutex
	seen      map[string]time.Time
	order     []string // LRU 顺序
	stopSweep chan struct{}
}

func NewDeduper(ttl time.Duration) *Deduper {
	return NewDeduperWithConfig(types.DedupConfig{TTL: ttl, MaxEntries: 10000, SweepInterval: 5 * time.Minute})
}

func NewDeduperWithConfig(cfg types.DedupConfig) *Deduper {
	if cfg.TTL <= 0 {
		cfg.TTL = types.DefaultDedupTTL
	}
	if cfg.MaxEntries <= 0 {
		cfg.MaxEntries = 10000
	}
	if cfg.SweepInterval <= 0 {
		cfg.SweepInterval = 5 * time.Minute
	}
	d := &Deduper{
		cfg:       cfg,
		seen:      make(map[string]time.Time),
		order:     make([]string, 0, cfg.MaxEntries),
		stopSweep: make(chan struct{}),
	}
	go d.sweepLoop()
	return d
}

func (d *Deduper) sweepLoop() {
	ticker := time.NewTicker(d.cfg.SweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			d.sweep()
		case <-d.stopSweep:
			return
		}
	}
}

func (d *Deduper) sweep() {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	for k, ts := range d.seen {
		if now.Sub(ts) > d.cfg.TTL {
			delete(d.seen, k)
		}
	}
	// Rebuild order without expired
	d.order = d.order[:0]
	for k := range d.seen {
		d.order = append(d.order, k)
	}
}

// CheckAndMark 命中（重复）返回 true。key 为空时跳过标记并返回 false。
func (d *Deduper) CheckAndMark(keys ...string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	// Check duplicates first
	for _, k := range keys {
		if k == "" {
			continue
		}
		if ts, ok := d.seen[k]; ok && now.Sub(ts) <= d.cfg.TTL {
			return true
		}
	}
	// Mark all keys
	for _, k := range keys {
		if k != "" {
			if _, exists := d.seen[k]; !exists {
				d.order = append(d.order, k)
			}
			d.seen[k] = now
		}
	}
	// LRU eviction
	for len(d.seen) > d.cfg.MaxEntries && len(d.order) > 0 {
		oldest := d.order[0]
		d.order = d.order[1:]
		delete(d.seen, oldest)
	}
	return false
}

func (d *Deduper) Dispose() {
	select {
	case <-d.stopSweep:
	default:
		close(d.stopSweep)
	}
}
