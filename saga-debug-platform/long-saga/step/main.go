// One step of a 20-step loan saga. The same binary runs as every service; STEP says which one.
//
// Choreography over one Kafka topic (loan.saga, keyed by saga id). Step i starts when step i-1
// reports done, and reports done or failed. A failure at step k is undone in reverse: step k-1
// undoes itself, then k-2 once k-1 reports undone, down to step 1. A stopped container simply
// stops answering; its consumer group resumes from the committed offset when it starts again.
//
// Step 1 also takes HTTP requests that open sagas: POST /start?count=N&gap=D&failAt=K&brokenUndo=J.
// failAt makes step K refuse; brokenUndo makes step J start its undo and never finish it.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type event struct {
	EventID    string `json:"eventId"`
	SagaID     string `json:"sagaId"`
	Type       string `json:"type"`
	Service    string `json:"serviceName"`
	OccurredAt string `json:"occurredAt"`
	FailAt     int    `json:"failAt,omitempty"`
	BrokenUndo int    `json:"brokenUndo,omitempty"`
}

const topic = "loan.saga"

var (
	steps   = strings.Split(os.Getenv("STEPS"), ",")
	step, _ = strconv.Atoi(os.Getenv("STEP"))
	service = os.Getenv("SERVICE")
	brokers = strings.Split(os.Getenv("KAFKA"), ",")
	writer  *kafka.Writer
)

func name(i int) string { return steps[i-1] }

func emit(sagaID, typ string, failAt, brokenUndo int) {
	e := event{uuid.NewString(), sagaID, typ, service, time.Now().UTC().Format(time.RFC3339Nano), failAt, brokenUndo}
	body, _ := json.Marshal(e)
	if err := writer.WriteMessages(context.Background(), kafka.Message{Key: []byte(sagaID), Value: body}); err != nil {
		log.Printf("publish %s %s: %v", sagaID, typ, err)
	}
}

// work stands for the step's own business call.
func work() { time.Sleep(time.Duration(150+rand.Intn(350)) * time.Millisecond) }

func forward(sagaID string, failAt, brokenUndo int) {
	emit(sagaID, name(step)+"_STARTED", failAt, brokenUndo)
	work()
	if failAt == step {
		emit(sagaID, name(step)+"_FAILED", failAt, brokenUndo)
	} else {
		emit(sagaID, name(step)+"_DONE", failAt, brokenUndo)
	}
}

func undo(sagaID string, failAt, brokenUndo int) {
	emit(sagaID, "UNDO_"+name(step)+"_STARTED", failAt, brokenUndo)
	if brokenUndo == step {
		return
	}
	work()
	emit(sagaID, "UNDO_"+name(step)+"_DONE", failAt, brokenUndo)
}

func react(e event) {
	if step > 1 && e.Type == name(step-1)+"_DONE" {
		go forward(e.SagaID, e.FailAt, e.BrokenUndo)
	}
	if step < len(steps) && (e.Type == name(step+1)+"_FAILED" || e.Type == "UNDO_"+name(step+1)+"_DONE") {
		go undo(e.SagaID, e.FailAt, e.BrokenUndo)
	}
}

func start(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	count, _ := strconv.Atoi(q.Get("count"))
	if count < 1 {
		count = 1
	}
	failAt, _ := strconv.Atoi(q.Get("failAt"))
	brokenUndo, _ := strconv.Atoi(q.Get("brokenUndo"))
	gap, _ := time.ParseDuration(q.Get("gap"))
	ids := make([]string, 0, count)
	for i := 0; i < count; i++ {
		id := "loan-" + uuid.NewString()[:8]
		ids = append(ids, id)
		go forward(id, failAt, brokenUndo)
		time.Sleep(gap)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(ids)
}

// ensureTopic creates the topic if it is not there yet. A group reader started before its topic
// exists is never assigned partitions, and that step would stay silent until restarted.
func ensureTopic() {
	for {
		conn, err := kafka.Dial("tcp", brokers[0])
		if err == nil {
			controller, cerr := conn.Controller()
			conn.Close()
			if cerr == nil {
				if cc, derr := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))); derr == nil {
					err = cc.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: 3, ReplicationFactor: 1})
					cc.Close()
					if err == nil || errors.Is(err, kafka.TopicAlreadyExists) {
						return
					}
				}
			}
		}
		log.Printf("waiting for kafka: %v", err)
		time.Sleep(2 * time.Second)
	}
}

func main() {
	if step < 1 || step > len(steps) || service == "" {
		log.Fatalf("STEP must be 1..%d and SERVICE set", len(steps))
	}
	ensureTopic()
	writer = &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic, Balancer: &kafka.Hash{},
		BatchTimeout: 5 * time.Millisecond, RequiredAcks: kafka.RequireOne}
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: topic,
		GroupID: service, StartOffset: kafka.FirstOffset, MaxWait: 100 * time.Millisecond})

	if step == 1 {
		http.HandleFunc("/start", start)
		go func() { log.Fatal(http.ListenAndServe(":8080", nil)) }()
	}
	log.Printf("%s is step %d (%s)", service, step, name(step))
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("read: %v", err)
			time.Sleep(time.Second)
			continue
		}
		var e event
		if json.Unmarshal(m.Value, &e) != nil {
			continue
		}
		react(e)
	}
}
