package cache

import (
	"testing"

	"github.com/lomik/go-carbon/points"
	"github.com/lomik/go-carbon/helper/metrics"
)

func TestInFlight(t *testing.T) {
	var data []points.Point
	tChan := make(chan metrics.MetricUpdate,5)
	c := New(tChan)

	c.Add(points.OnePoint("hello.world", 42, 10))

	m1 := c.WriteoutQueue().Get(nil)
	p1, _ := c.PopNotConfirmed(m1)
	if !p1.Eq(points.OnePoint("hello.world", 42, 10)) {
		t.FailNow()
	}

	data = c.Get("hello.world")
	if len(data) != 1 || data[0].Value != 42 {
		t.FailNow()
	}

	c.Add(points.OnePoint("hello.world", 43, 10))

	// 42 in flight, 43 in cache
	data = c.Get("hello.world")
	if len(data) != 2 || data[0].Value != 42 || data[1].Value != 43 {
		t.FailNow()
	}

	m2 := c.WriteoutQueue().Get(nil)
	p2, _ := c.PopNotConfirmed(m2)
	if !p2.Eq(points.OnePoint("hello.world", 43, 10)) {
		t.FailNow()
	}

	// 42, 43 in flight
	data = c.Get("hello.world")
	if len(data) != 2 || data[0].Value != 42 || data[1].Value != 43 {
		t.FailNow()
	}

	c.Confirm(p1)

	c.Add(points.OnePoint("hello.world", 44, 10))
	m3 := c.WriteoutQueue().Get(nil)
	p3, _ := c.PopNotConfirmed(m3)
	if !p3.Eq(points.OnePoint("hello.world", 44, 10)) {
		t.FailNow()
	}

	// 43, 44 in flight
	data = c.Get("hello.world")
	if len(data) != 2 || data[0].Value != 43 || data[1].Value != 44 {
		t.FailNow()
	}
	close(tChan)
}

func BenchmarkPopNotConfirmed(b *testing.B) {
	tChan := make(chan metrics.MetricUpdate,5)
	c := New(tChan)
	p1 := points.OnePoint("hello.world", 42, 10)
	var p2 *points.Points

	for n := 0; n < b.N; n++ {
		c.Add(p1)
		p2, _ = c.PopNotConfirmed("hello.world")
		c.Confirm(p2)
	}

	if !p1.Eq(p2) {
		b.FailNow()
	}
	close(tChan)
}

func BenchmarkPopNotConfirmed100(b *testing.B) {
	tChan := make(chan metrics.MetricUpdate,5)
	c := New(tChan)

	for i := 0; i < 100; i++ {
		c.Add(points.OnePoint("hello.world", 42, 10))
		c.PopNotConfirmed("hello.world")
	}

	p1 := points.OnePoint("hello.world", 42, 10)
	var p2 *points.Points

	for n := 0; n < b.N; n++ {
		c.Add(p1)
		p2, _ = c.PopNotConfirmed("hello.world")
		c.Confirm(p2)
	}

	if !p1.Eq(p2) {
		b.FailNow()
	}
	close(tChan)
}

func BenchmarkPop(b *testing.B) {
	tChan := make(chan metrics.MetricUpdate,5)
	c := New(tChan)
	p1 := points.OnePoint("hello.world", 42, 10)
	var p2 *points.Points

	for n := 0; n < b.N; n++ {
		c.Add(p1)
		p2, _ = c.Pop("hello.world")
	}

	if !p1.Eq(p2) {
		b.FailNow()
	}
	close(tChan)
}
func BenchmarkGet(b *testing.B) {
	tChan := make(chan metrics.MetricUpdate,5)
	c := New(tChan)
	c.Add(points.OnePoint("hello.world", 42, 10))

	var d []points.Point
	for n := 0; n < b.N; n++ {
		d = c.Get("hello.world")
	}

	if len(d) != 1 {
		b.FailNow()
	}
	close(tChan)
}

func BenchmarkGetNotConfirmed1(b *testing.B) {
	tChan := make(chan metrics.MetricUpdate,5)
	c := New(tChan)

	c.Add(points.OnePoint("hello.world", 42, 10))
	c.PopNotConfirmed("hello.world")

	var d []points.Point
	for n := 0; n < b.N; n++ {
		d = c.Get("hello.world")
	}

	if len(d) != 1 {
		b.FailNow()
	}
	close(tChan)
}

func BenchmarkGetNotConfirmed100(b *testing.B) {
	tChan := make(chan metrics.MetricUpdate,5)
	c := New(tChan)

	for i := 0; i < 100; i++ {
		c.Add(points.OnePoint("hello.world", 42, 10))
		c.PopNotConfirmed("hello.world")
	}

	var d []points.Point
	for n := 0; n < b.N; n++ {
		d = c.Get("hello.world")
	}

	if len(d) != 100 {
		b.FailNow()
	}
	close(tChan)
}

func BenchmarkGetNotConfirmed100Miss(b *testing.B) {
	tChan := make(chan metrics.MetricUpdate,5)
	c := New(tChan)

	for i := 0; i < 100; i++ {
		c.Add(points.OnePoint("hello.world", 42, 10))
		c.PopNotConfirmed("hello.world")
	}

	var d []points.Point
	for n := 0; n < b.N; n++ {
		d = c.Get("metric.name")
	}

	if d != nil {
		b.FailNow()
	}
	close(tChan)
}
