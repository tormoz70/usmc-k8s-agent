// mock-core sends Kafka commands and listens for replies/events (local testing).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/usmc/usmc-k8s-agent/hack/mockcorelib"
)

func main() {
	brokers := flag.String("brokers", env("KAFKA_BROKERS", "localhost:9092"), "Kafka brokers")
	requestTopic := flag.String("request-topic", mockcorelib.DefaultRequestTopic, "request topic")
	replyTopic := flag.String("reply-topic", mockcorelib.DefaultReplyTopic, "reply topic")
	eventTopic := flag.String("topic", "", "topic to listen on (with -listen)")
	bodyFile := flag.String("file", "", "JSON command body file")
	listen := flag.Bool("listen", false, "listen on a topic continuously")
	flag.Parse()

	brokerList := mockcorelib.SplitBrokers(*brokers)
	if *listen {
		topic := *eventTopic
		if topic == "" {
			topic = *replyTopic
		}
		fmt.Printf("listening on %s ...\n", topic)
		err := mockcorelib.ListenTopic(brokerList, topic, func(m mockcorelib.Message) {
			fmt.Printf("[%s] partition=%d offset=%d key=%q correlation_id=%q body=%s\n",
				m.Topic, m.Partition, m.Offset, m.Key, m.CorrelationID, string(m.Body))
		})
		fatal(err)
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

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	result, err := mockcorelib.SendCommand(ctx, brokerList, *requestTopic, *replyTopic, data)
	if err != nil {
		fatal(err)
	}
	fmt.Printf("sent command correlation_id=%s reply_topic=%s\n", result.CorrelationID, result.ReplyTopic)

	go func() {
		err := mockcorelib.ListenOnce(brokerList, result.ReplyTopic, result.CorrelationID, 2*time.Minute, func(m mockcorelib.Message) {
			fmt.Printf("[%s] partition=%d offset=%d key=%q body=%s\n",
				m.Topic, m.Partition, m.Offset, m.Key, string(m.Body))
		})
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		os.Exit(0)
	}()
	select {}
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
