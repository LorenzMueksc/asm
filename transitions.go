package asm

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"rumue.io/asm/service"
)

type (
	Transition struct {
		// Remote indicates that this node does not own the transition.
		// Matching events are forwarded to one of the NodeIds.
		Remote bool
		// NodeIds of the nodes owning this transition with the same name and signature.
		NodeIds []string
		// Name of the transition. Transitions with the same name must have the same signature in the cluster.
		Name string
		// EventName is the name of the event that triggers the transition.
		EventName string
		// Priority of the transition. Higher priority transitions are executed first.
		Priority int
		// SaveOnChange indicates if the state machine should execute the SaveFunc after the transition.
		SaveOnChange bool
		// Conditions to evaluate to determine if the transition should be executed.
		Conditions []*Condition
		// Actions to execute if the condition applies. Either a transitionFunc or the Bits of a target state.
		Actions []any
		// Batching
		Batching Batching
		// Signature identifies the behavior of the transition: event name, conditions and action.
		Signature string
		// Entity is the name of the root entity bitmap the transition runs on. Transitions of another entity than
		// the node's are remote: events matching them go to the containers of their owners.
		Entity string

		// funcKey identifies the func of a func transition in the signature.
		// Defaults to the runtime name of the func.
		funcKey string
		// toState is the example entity of a state transition.
		// It is compiled to the state action against the committed bitmap.
		toState any
		// stateBitmap is the name of the bitmap of the target state of a state transition.
		stateBitmap string
	}
	Batching struct {
		// Count is the number of events it waits for before executing the transition.
		Count int
		// Period is the time to wait before executing the transition after the first event.
		Period time.Duration
	}
	Condition struct {
		// States is a list of states per entity that the condition applies to.
		States map[string][]Bits
		// Operator is the operator to use for the condition, eg. All, None, OneOf, AtLeastOne.
		Operator Operator

		// entities the condition was built from.
		// They are compiled to States against the committed bitmap.
		entities []any
	}
	Operator         int32
	TransitionOption interface {
		apply(*Transition)
	}
	// funcTransitionOption wraps a function that modifies it into an
	// implementation of the TransitionOption interface.
	funcTransitionOption struct {
		f func(*Transition)
	}
)

const (
	All Operator = iota
	None
	OneOf
	AtLeastOne
)

func (fdo *funcTransitionOption) apply(do *Transition) {
	fdo.f(do)
}

func newFuncTransitionOption(f func(*Transition)) *funcTransitionOption {
	return &funcTransitionOption{
		f: f,
	}
}

// WithPriority sets the priority of the transition. Higher priority transitions are executed first.
func WithPriority(priority int) TransitionOption {
	return newFuncTransitionOption(func(to *Transition) {
		to.Priority = priority
	})
}

// WithSaveAfter sets the Transition to execute the SaveFunc after the transition.
// This is useful if the transition modifies the state machine, and it should only execute a save once after all transitions.
//
// Note: This is not the default behavior. The SaveFunc is only executed if the transition
// is executed locally. If the transition is executed remotely, the SaveFunc is not executed.
func WithSaveAfter() TransitionOption {
	return newFuncTransitionOption(func(to *Transition) {
		to.SaveOnChange = true
	})
}

// WithConditions sets the conditions to evaluate to determine if the transition should be executed.
func WithConditions(conditions ...*Condition) TransitionOption {
	return newFuncTransitionOption(func(to *Transition) {
		to.Conditions = append(to.Conditions, conditions...)
	})
}

func WithBatching(count int, period time.Duration) TransitionOption {
	return newFuncTransitionOption(func(transition *Transition) {
		if count > 0 {
			transition.Batching = Batching{
				Count:  count,
				Period: period,
			}
		}
	})
}

// WithFuncKey sets the key identifying the func of a func transition in its signature.
// Without it the runtime name of the func is used, which changes for closures
// when other closures are added or removed around them.
func WithFuncKey(key string) TransitionOption {
	return newFuncTransitionOption(func(transition *Transition) {
		transition.funcKey = key
	})
}

// funcName returns the runtime name of a func, eg. "myapp/handlers.Promote".
func funcName(f any) string {
	fn := runtime.FuncForPC(reflect.ValueOf(f).Pointer())
	if fn == nil {
		return ""
	}
	return fn.Name()
}

// isFuncTransition reports whether the transition executes a func.
// Remote func transitions have no actions on this node.
func (t *Transition) isFuncTransition() bool {
	return t.toState == nil && t.stateBitmap == ""
}

// stateAction returns the target state of a state transition.
func (t *Transition) stateAction() (Bits, bool) {
	for _, action := range t.Actions {
		if bits, ok := action.(Bits); ok {
			return bits, true
		}
	}
	return nil, false
}

// computeSignature hashes the event name, the conditions and the action of the transition, described by their
// example entities instead of the compiled bits: the bits change when the committed bitmap grows, the example
// entities don't, so nodes owning the same transition agree no matter when they compile it.
// Conditions are hashed independent of their order.
func (t *Transition) computeSignature() string {
	h := sha256.New()
	writeString := func(s string) {
		_ = binary.Write(h, binary.BigEndian, uint64(len(s)))
		h.Write([]byte(s))
	}

	writeString(t.EventName)

	conditions := make([]string, 0, len(t.Conditions))
	for _, c := range t.Conditions {
		conditions = append(conditions, c.canonical())
	}
	slices.Sort(conditions)
	for _, c := range conditions {
		writeString(c)
	}

	switch {
	case t.isFuncTransition():
		writeString("func:" + t.funcKey)
	case t.toState != nil:
		writeString("state:" + describeEntity(t.toState))
	default:
		state, _ := t.stateAction()
		writeString("state:" + t.stateBitmap + ":" + bitsString(state))
	}

	return hex.EncodeToString(h.Sum(nil))
}

// canonical returns an order independent description of the condition by its example entities.
// A condition built without example entities is described by its states.
func (c *Condition) canonical() string {
	if c.entities == nil {
		return "states:" + c.statesKey()
	}
	entities := make([]string, 0, len(c.entities))
	for _, e := range c.entities {
		entities = append(entities, describeEntity(e))
	}
	slices.Sort(entities)
	return fmt.Sprintf("%d|%s", c.Operator, strings.Join(entities, ","))
}

// statesKey returns an order independent description of the compiled states of the condition.
func (c *Condition) statesKey() string {
	names := make([]string, 0, len(c.States))
	for name := range c.States {
		names = append(names, name)
	}
	slices.Sort(names)

	var sb strings.Builder
	fmt.Fprintf(&sb, "%d", c.Operator)
	for _, name := range names {
		states := make([]string, 0, len(c.States[name]))
		for _, state := range c.States[name] {
			states = append(states, bitsString(state))
		}
		slices.Sort(states)
		fmt.Fprintf(&sb, "|%s=%s", name, strings.Join(states, ","))
	}
	return sb.String()
}

// compiledKey describes the compiled bits of the conditions and the target state. It changes when the committed
// bitmap gives values of the example entities new bits, then the other nodes need the transition again.
func (t *Transition) compiledKey() string {
	keys := make([]string, 0, len(t.Conditions)+1)
	for _, c := range t.Conditions {
		keys = append(keys, c.statesKey())
	}
	slices.Sort(keys)
	if state, ok := t.stateAction(); ok {
		keys = append(keys, "state:"+t.stateBitmap+":"+bitsString(state))
	}
	return strings.Join(keys, ";")
}

// describeEntity returns the type and the field values of an example entity.
// Fields with their zero value are left out, which describes the same entity.
func describeEntity(e any) string {
	b, err := json.Marshal(e)
	if err != nil {
		return fmt.Sprintf("%T%#v", e, e)
	}
	return fmt.Sprintf("%T%s", e, b)
}

func bitsString(b Bits) string {
	words := make([]string, 0, len(b))
	for _, w := range b.trim() {
		words = append(words, fmt.Sprintf("%x", w))
	}
	return strings.Join(words, ".")
}

var operatorToProto = map[Operator]service.Operator{
	All:        service.Operator_ALL,
	None:       service.Operator_NONE,
	OneOf:      service.Operator_ONE_OF,
	AtLeastOne: service.Operator_AT_LEAST_ONE,
}

func operatorFromProto(op service.Operator) Operator {
	for k, v := range operatorToProto {
		if v == op {
			return k
		}
	}
	return Operator(-1)
}

// toProto converts the compiled transition to its wire format for syncing it to other nodes.
func (t *Transition) toProto(nodeId string) *service.Transition {
	conditions := make([]*service.Transition_Condition, 0, len(t.Conditions))
	for _, c := range t.Conditions {
		states := make(map[string]*service.Transition_Condition_States, len(c.States))
		for name, list := range c.States {
			ps := &service.Transition_Condition_States{}
			for _, state := range list {
				ps.States = append(ps.States, state.toProto())
			}
			states[name] = ps
		}
		conditions = append(conditions, &service.Transition_Condition{
			States:   states,
			Operator: operatorToProto[c.Operator],
		})
	}

	// the wire field is int32: priorities beyond its range are clamped instead of wrapping around
	priority := int32(min(max(t.Priority, math.MinInt32), math.MaxInt32))
	pt := &service.Transition{
		Name:         t.Name,
		EventName:    t.EventName,
		Priority:     priority,
		SaveOnChange: t.SaveOnChange,
		NodeId:       nodeId,
		Conditions:   conditions,
		Batching: &service.Transition_Batching{
			Count:  uint64(max(t.Batching.Count, 0)),
			Period: durationpb.New(t.Batching.Period),
		},
		Signature: t.Signature,
		Entity:    t.Entity,
	}
	if !t.isFuncTransition() {
		state, _ := t.stateAction()
		pt.State = &service.Transition_State{
			Bitmap: t.stateBitmap,
			Bits:   state.toProto(),
		}
	}

	return pt
}

// transitionFromProto converts a transition received from another node.
// Func transitions become remote transitions, state transitions run locally.
func transitionFromProto(pt *service.Transition) *Transition {
	conditions := make([]*Condition, 0, len(pt.GetConditions()))
	for _, pc := range pt.GetConditions() {
		states := make(map[string][]Bits, len(pc.GetStates()))
		for name, ps := range pc.GetStates() {
			for _, state := range ps.GetStates() {
				states[name] = append(states[name], bitsFromProto(state))
			}
		}
		conditions = append(conditions, &Condition{
			States:   states,
			Operator: operatorFromProto(pc.GetOperator()),
		})
	}

	t := &Transition{
		NodeIds:      []string{pt.GetNodeId()},
		Name:         pt.GetName(),
		EventName:    pt.GetEventName(),
		Priority:     int(pt.GetPriority()),
		SaveOnChange: pt.GetSaveOnChange(),
		Conditions:   conditions,
		Batching: Batching{
			Count:  int(pt.GetBatching().GetCount()),
			Period: pt.GetBatching().GetPeriod().AsDuration(),
		},
		Signature: pt.GetSignature(),
		Entity:    pt.GetEntity(),
	}
	if pt.GetState() != nil {
		t.stateBitmap = pt.GetState().GetBitmap()
		t.Actions = []any{bitsFromProto(pt.GetState().GetBits())}
	} else {
		t.Remote = true
	}

	return t
}
