package kabaka

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
)

type Message struct {
	ID       uuid.UUID
	Value    []byte
	Retry    int
	CreateAt time.Time
	Headers  map[string]string
	RootSpan trace.Span
}

func (m *Message) Get(key string) string {
	for mapkey, value := range m.Headers {
		if key == mapkey {
			return value
		}
	}

	return ""
}

func (m *Message) Set(key string, value string) {
	m.Headers[key] = value
}

func (m *Message) Keys() []string {
	var keys []string

	for key := range m.Headers {
		keys = append(keys, m.Headers[key])
	}

	return keys
}

func (m *Message) initTrace(
	topicName string, contextProvider propagation.TextMapCarrier,
) {
	propagator := otel.GetTextMapPropagator()
	provider := otel.GetTracerProvider()

	tracer := provider.Tracer(
		defaultTraceName,
		trace.WithInstrumentationVersion(version),
	)

	if contextProvider == nil {
		contextProvider = propagation.MapCarrier(m.Headers)
	}

	parentCtx := propagator.Extract(context.Background(), contextProvider)

	opts := []trace.SpanStartOption{
		trace.WithAttributes(
			semconv.MessagingDestinationKey.String(topicName),
		),
		trace.WithSpanKind(trace.SpanKindProducer),
	}

	traceName := fmt.Sprintf("send message to %s", topicName)
	ctx, span := tracer.Start(parentCtx, traceName, opts...)

	propagator.Inject(ctx, m)

	m.RootSpan = span
}
