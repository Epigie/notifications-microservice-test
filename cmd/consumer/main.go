package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"

	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/Shopify/sarama"
	"github.com/vsouza/go-kafka/example/models"
	"github.com/vsouza/go-kafka/example/pkg/kafka"
)

var (
	brokerList        = kingpin.Flag("brokerList", "List of brokers to connect").Default("broker:9092").Strings()
	topic             = kingpin.Flag("topic", "Topic name").Default("notifications").String()
	partition         = kingpin.Flag("partition", "Partition number").Default("0").String()
	offsetType        = kingpin.Flag("offsetType", "Offset Type (OffsetNewest | OffsetOldest)").Default("-1").Int()
	messageCountStart = kingpin.Flag("messageCountStart", "Message counter start from:").Int()
)

func main() {
	kingpin.Parse()
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	brokers := *brokerList
	master, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Panic(err)
	}
	log.Printf("Successfully connected to Kafka brokers: %v", brokers)
	defer func() {
		if err := master.Close(); err != nil {
			log.Panic(err)
		}
	}()

	// Get all partitions for the topic
	partitions, err := master.Partitions(*topic)
	if err != nil {
		log.Panic(err)
	}
	log.Printf("Found %d partitions for topic '%s': %v", len(partitions), *topic, partitions)

	// Create a consumer for each partition
	var consumers []sarama.PartitionConsumer
	for _, partition := range partitions {
		pc, err := master.ConsumePartition(*topic, partition, sarama.OffsetOldest)
		if err != nil {
			log.Panic(err)
		}
		consumers = append(consumers, pc)
		log.Printf("Successfully started consuming from topic '%s' on partition %d", *topic, partition)
	}

	// Close all partition consumers on exit
	defer func() {
		for _, pc := range consumers {
			if err := pc.Close(); err != nil {
				log.Printf("Error closing partition consumer: %v", err)
			}
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	doneCh := make(chan struct{})

	// Start a goroutine for each partition consumer
	for _, consumer := range consumers {
		go func(pc sarama.PartitionConsumer) {
			for {
				select {
				case err := <-pc.Errors():
					log.Println(err)
				case msg := <-pc.Messages():
					*messageCountStart++
					log.Printf("Received message from partition %d: %s", msg.Partition, string(msg.Value))

					var message models.Message
					if err := json.Unmarshal(msg.Value, &message); err != nil {
						log.Printf("Error unmarshaling message: %v", err)
						continue
					}

					// modify the message status and content
					message.Status = "processed"
					message.Content = "Notification has been sent successfully"

					if err := kafka.PushToKafka(*brokerList, "database", 5, message); err != nil {
						log.Printf("Error pushing to Kafka: %v", err)
					}
				}
			}
		}(consumer)
	}

	// Wait for interrupt signal
	go func() {
		<-signals
		log.Println("Interrupt is detected")
		doneCh <- struct{}{}
	}()

	<-doneCh
	log.Println("Processed", *messageCountStart, "messages")
}
