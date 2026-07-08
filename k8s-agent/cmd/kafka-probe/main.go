package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	brokers := flag.String("brokers", envOr("KAFKA_BROKERS", "localhost:9092"), "Comma-separated Kafka brokers")
	action := flag.String("action", "consume", "send or consume")
	topic := flag.String("topic", "commands.results", "Kafka topic")
	messageFile := flag.String("file", "", "JSON file to send (send action)")
	key := flag.String("key", "", "Message key for send")
	timeout := flag.Duration("timeout", 30*time.Second, "Consume timeout")
	fromBeginning := flag.Bool("from-beginning", false, "Consume from beginning of topic")
	flag.Parse()

	brokerList := splitCSV(*brokers)
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	switch *action {
	case "send":
		if *messageFile == "" {
			log.Fatal("-file is required for send")
		}
		data, err := os.ReadFile(*messageFile)
		if err != nil {
			log.Fatalf("read file: %v", err)
		}
		w := &kafka.Writer{
			Addr:         kafka.TCP(brokerList...),
			Topic:        *topic,
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireAll,
		}
		defer w.Close()
		msg := kafka.Message{Value: data, Time: time.Now().UTC()}
		if *key != "" {
			msg.Key = []byte(*key)
		}
		if err := w.WriteMessages(ctx, msg); err != nil {
			log.Fatalf("send: %v", err)
		}
		fmt.Fprintf(os.Stderr, "sent %d bytes to topic %s\n", len(data), *topic)
	case "consume":
		startOffset := kafka.LastOffset
		if *fromBeginning {
			startOffset = kafka.FirstOffset
		}
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:     brokerList,
			Topic:       *topic,
			StartOffset: startOffset,
			MaxBytes:    10e6,
		})
		defer r.Close()
		for {
			msg, err := r.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Fatalf("consume: %v", err)
			}
			if msg.Key != nil {
				fmt.Fprintf(os.Stderr, "key=%s\n", string(msg.Key))
			}
			_, _ = os.Stdout.Write(msg.Value)
			fmt.Println()
		}
	default:
		log.Fatalf("unknown action: %s", *action)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func splitCSV(s string) []string {
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