// mock-core sends Kafka commands and listens for replies/events (local testing).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

func main() {
	brokers := flag.String("brokers", env("KAFKA_BROKERS", "localhost:9092"), "Kafka brokers")
	requestTopic := flag.String("request-topic", "k8s.commands.request", "request topic")
	replyTopic := flag.String("reply-topic", "core-client.dev.responses", "reply topic")
	eventTopic := flag.String("topic", "", "topic to listen on (with -listen)")
	bodyFile := flag.String("file", "", "JSON command body file")
	listen := flag.Bool("listen", false, "listen on a topic continuously")
	flag.Parse()

	brokerList := splitBrokers(*brokers)
	if *listen {
		topic := *eventTopic
		if topic == "" {
			topic = *replyTopic
		}
		listenTopic(brokerList, topic)
		return
	}
	if *bodyFile == "" {
		fmt.Fprintln(os.Stderr, "usage:")
		fmt.Fprintln(os.Stderr, "  mock-core -file command.json [-reply-topic ...]")
		fmt.Fprintln(os.Stderr, "  mock-core -listen [-topic cluster.events|logs.stream|agent.lifecycle|...]")
		os.Exit(2)
	}
	data, err := os.ReadFile(*bodyFile)
	if err != nil {
		fatal(err)
	}
	sendCommand(brokerList, *requestTopic, *replyTopic, data)
}

func sendCommand(brokers []string, requestTopic, replyTopic string, body []byte) {
	corrID := fmt.Sprintf("corr-%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	go listenOnce(brokers, replyTopic, corrID)

	w := &kafkago.Writer{
		Addr:     kafkago.TCP(brokers...),
		Topic:    requestTopic,
		Balancer: &kafkago.LeastBytes{},
	}
	defer w.Close()

	err := w.WriteMessages(ctx, kafkago.Message{
		Value: body,
		Headers: []kafkago.Header{
			{Key: "correlation_id", Value: []byte(corrID)},
			{Key: "reply_topic", Value: []byte(replyTopic)},
		},
	})
	if err != nil {
		fatal(err)
	}
	fmt.Printf("sent command correlation_id=%s reply_topic=%s\n", corrID, replyTopic)
	select {}
}

func listenTopic(brokers []string, topic string) {
	r := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: fmt.Sprintf("mock-core-%d", time.Now().UnixNano()),
	})
	defer r.Close()
	fmt.Printf("listening on %s ...\n", topic)
	for {
		msg, err := r.ReadMessage(context.Background())
		if err != nil {
			fatal(err)
		}
		printMessage(topic, msg)
	}
}

func listenOnce(brokers []string, replyTopic, corrID string) {
	r := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers,
		Topic:   replyTopic,
		GroupID: fmt.Sprintf("mock-core-%d", time.Now().UnixNano()),
	})
	defer r.Close()
	deadline := time.After(2 * time.Minute)
	for {
		select {
		case <-deadline:
			fmt.Println("timeout waiting for reply")
			os.Exit(1)
		default:
		}
		msg, err := r.ReadMessage(context.Background())
		if err != nil {
			return
		}
		if headerValue(msg.Headers, "correlation_id") != corrID {
			continue
		}
		printMessage(replyTopic, msg)
		os.Exit(0)
	}
}

func printMessage(topic string, msg kafkago.Message) {
	var pretty json.RawMessage
	if json.Valid(msg.Value) {
		pretty = msg.Value
	} else {
		pretty = json.RawMessage(fmt.Sprintf("%q", string(msg.Value)))
	}
	fmt.Printf("[%s] partition=%d offset=%d key=%q body=%s\n", topic, msg.Partition, msg.Offset, string(msg.Key), string(pretty))
}

func headerValue(headers []kafkago.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func splitBrokers(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
