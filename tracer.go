// tracer.go
package kabaka

import (
	"context"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type SpanKind int

const (
	SpanKindProducer trace.SpanKind = iota
	SpanKindConsumer
)

type SpanContext struct {
	TraceID    string
	SpanID     string
	ParentID   string
	Properties map[string]string
}

type Span interface {
	End()
	SetError(err error)
	SetAttribute(key string, value interface{})
	Context() SpanContext
}

type SpanOption func(*SpanOptions)

type SpanOptions struct {
	Kind       SpanKind
	Attributes map[string]interface{}
}

type Tracer interface {
	// StartSpan starts a new span
	StartSpan(ctx context.Context, name string, opts ...SpanOption) (context.Context, Span)

	// Inject injects span context into carrier
	Inject(ctx context.Context, carrier map[string]string)

	// Extract extracts span context from carrier
	Extract(ctx context.Context, carrier map[string]string) context.Context
}

type DefaultSpan struct{}

func (s *DefaultSpan) End()                                       {}
func (s *DefaultSpan) SetError(err error)                         {}
func (s *DefaultSpan) SetAttribute(key string, value interface{}) {}
func (s *DefaultSpan) Context() SpanContext {
	return SpanContext{}
}

// DefaultTracer is a tracer that does nothing
type DefaultTracer struct{}

func (t *DefaultTracer) StartSpan(ctx context.Context, name string, opts ...SpanOption) (context.Context, Span) {
	return ctx, &DefaultSpan{}
}

func (t *DefaultTracer) Inject(ctx context.Context, carrier map[string]string) {}

func (t *DefaultTracer) Extract(ctx context.Context, carrier map[string]string) context.Context {
	return ctx
}

// OtelSpan wraps OpenTelemetry span
type OtelSpan struct {
	span trace.Span
}

func (s *OtelSpan) End() {
	s.span.End()
}

func (s *OtelSpan) SetError(err error) {
	s.span.RecordError(err)
}

func (s *OtelSpan) SetAttribute(key string, value interface{}) {
	// Convert and set OpenTelemetry attribute
}

func (s *OtelSpan) Context() SpanContext {
	// Convert OpenTelemetry SpanContext to our SpanContext
	return SpanContext{}
}

// OtelTracer wraps OpenTelemetry tracer
type OtelTracer struct {
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
}

func NewOtelTracer(tracer trace.Tracer, propagator propagation.TextMapPropagator) *OtelTracer {
	return &OtelTracer{
		tracer:     tracer,
		propagator: propagator,
	}
}

func (t *OtelTracer) StartSpan(ctx context.Context, name string, opts ...SpanOption) (context.Context, Span) {
	// Convert our options to OpenTelemetry options
	options := &SpanOptions{}
	for _, opt := range opts {
		opt(options)
	}

	ctx, span := t.tracer.Start(ctx, name)
	return ctx, &OtelSpan{span: span}
}

func (t *OtelTracer) Inject(ctx context.Context, carrier map[string]string) {
	t.propagator.Inject(ctx, propagation.MapCarrier(carrier))
}

func (t *OtelTracer) Extract(ctx context.Context, carrier map[string]string) context.Context {
	return t.propagator.Extract(ctx, propagation.MapCarrier(carrier))
}

// Helper functions for span options
func WithSpanKind(kind SpanKind) SpanOption {
	return func(o *SpanOptions) {
		o.Kind = kind
	}
}

func WithAttribute(key string, value interface{}) SpanOption {
	return func(o *SpanOptions) {
		if o.Attributes == nil {
			o.Attributes = make(map[string]interface{})
		}
		o.Attributes[key] = value
	}
}
