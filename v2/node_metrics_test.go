package asm

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
	"rumue.io/asm/service"
)

// metricValue returns the value of a counter or gauge, or the count of a histogram.
func metricValue(ms []*service.Metric, name string) float64 {
	for _, m := range ms {
		if m.GetName() == name {
			if m.GetKind() == service.Metric_HISTOGRAM {
				return float64(m.GetCount())
			}
			return m.GetValue()
		}
	}
	return -1
}

var _ = Describe("Node metrics", func() {
	It("should count observations per bucket", func() {
		var h histogram
		h.observe(50 * time.Microsecond)
		h.observe(100 * time.Microsecond)
		h.observe(3 * time.Millisecond)
		h.observe(time.Minute)
		Expect(h.counts[0].Load()).To(Equal(uint64(2)))
		Expect(h.counts[5].Load()).To(Equal(uint64(1)))
		Expect(h.counts[len(histogramBounds)].Load()).To(Equal(uint64(1)))
		Expect(time.Duration(h.sumNanos.Load())).To(Equal(time.Minute + 3150*time.Microsecond))
	})

	It("should write the Prometheus text format", func() {
		var buf bytes.Buffer
		writePrometheus(&buf, []*service.Metric{
			{Name: "a_total", Help: "a counter", Kind: service.Metric_COUNTER, Value: 3},
			{Name: "b", Help: "a gauge", Kind: service.Metric_GAUGE, Value: 1.5},
			{Name: "c_seconds", Help: "a histogram", Kind: service.Metric_HISTOGRAM, Bounds: []float64{0.1, 1}, Counts: []uint64{2, 1, 1}, Sum: 3.5, Count: 4},
		})
		Expect(buf.String()).To(Equal(`# HELP a_total a counter
# TYPE a_total counter
a_total 3
# HELP b a gauge
# TYPE b gauge
b 1.5
# HELP c_seconds a histogram
# TYPE c_seconds histogram
c_seconds_bucket{le="0.1"} 2
c_seconds_bucket{le="1"} 3
c_seconds_bucket{le="+Inf"} 4
c_seconds_sum 3.5
c_seconds_count 4
`))
	})

	It("should return the metrics and at most limit containers in Inspect, and serve the metrics", func() {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		Expect(err).NotTo(HaveOccurred())
		addr := l.Addr().String()
		Expect(l.Close()).To(Succeed())

		bitmap, _ := createSimpleTestBitmap()
		n := NewNode(1, func(entity any, bitMap *Bitmap) *Bitmap { return bitMap }, func() *mockBuilder { return &mockBuilder{} }, "",
			WithSkipRaft(), WithBitmap(bitmap), WithLogger(zap.NewNop()), WithMetricsAddr(addr))
		defer n.Shutdown()
		n.AddStateTransition("process", "STEP", &TestStateEntity{Status: "processing"})

		for i := range 3 {
			c, _, err := n.StartContainer(context.Background(), fmt.Sprintf("c%d", i))
			Expect(err).NotTo(HaveOccurred())
			c.Builder().Set(&TestStateEntity{Status: "pending"})
			doneC := make(chan struct{}, 1)
			Expect(c.Event("STEP", &TestEvent{Value: "v"}, WithDoneChannel(doneC))).To(Succeed())
			Eventually(doneC).Should(Receive())
		}

		resp, err := n.Inspect(context.Background(), &service.InspectRequest{Limit: 2})
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.GetContainers()).To(HaveLen(2))
		Expect(resp.GetContainers()[0].GetId()).To(Equal("c0"))
		Expect(resp.GetContainerCount()).To(Equal(uint32(3)))
		Expect(metricValue(resp.GetMetrics(), "asm_containers_open")).To(Equal(3.0))
		Expect(metricValue(resp.GetMetrics(), "asm_containers_started_total")).To(Equal(3.0))
		Expect(metricValue(resp.GetMetrics(), "asm_events_total")).To(Equal(3.0))
		Expect(metricValue(resp.GetMetrics(), "asm_event_seconds")).To(Equal(3.0))
		Expect(metricValue(resp.GetMetrics(), "go_goroutines")).To(BeNumerically(">", 0))

		all, err := n.Inspect(context.Background(), &service.InspectRequest{})
		Expect(err).NotTo(HaveOccurred())
		Expect(all.GetContainers()).To(HaveLen(3))

		By("serving the metrics in the Prometheus text format")
		Eventually(func() string {
			r, err := http.Get("http://" + addr + "/metrics")
			if err != nil {
				return err.Error()
			}
			defer r.Body.Close()
			body, _ := io.ReadAll(r.Body)
			return string(body)
		}).Should(And(ContainSubstring("asm_events_total 3\n"), ContainSubstring(`asm_event_seconds_bucket{le="+Inf"} 3`)))
	})

	It("should serve the metrics with MetricsHandler, collected at most once per second", func() {
		bitmap, _ := createSimpleTestBitmap()
		n := NewNode(1, func(entity any, bitMap *Bitmap) *Bitmap { return bitMap }, func() *mockBuilder { return &mockBuilder{} }, "",
			WithSkipRaft(), WithBitmap(bitmap), WithLogger(zap.NewNop()))
		defer n.Shutdown()
		Eventually(n.Ready()).Should(BeClosed())

		scrape := func() string {
			rec := httptest.NewRecorder()
			n.MetricsHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
			Expect(rec.Code).To(Equal(http.StatusOK))
			Expect(rec.Header().Get("Content-Type")).To(HavePrefix("text/plain; version=0.0.4"))
			return rec.Body.String()
		}

		before := time.Now()
		first := scrape()
		Expect(first).To(ContainSubstring("# TYPE asm_containers_started_total counter\nasm_containers_started_total 0\n"))
		_, _, err := n.StartContainer(context.Background(), "c1")
		Expect(err).NotTo(HaveOccurred())
		second := scrape()
		// collected again only once the output is a second old
		if time.Since(before) < metricsCacheTTL {
			Expect(second).To(Equal(first))
		}
		Eventually(scrape, 3*time.Second).Should(ContainSubstring("asm_containers_started_total 1\n"))
	})
})
