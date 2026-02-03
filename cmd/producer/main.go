package main

import (
	"log"
	"net/http"

	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/gin-gonic/gin"
	"github.com/vsouza/go-kafka/example/models"
	"github.com/vsouza/go-kafka/example/pkg/kafka"
)

var (
	brokerList = kingpin.Flag("brokerList", "List of brokers to connect").Default("localhost:9094").Strings()
	topic      = kingpin.Flag("topic", "Topic name").Default("database").String()
	maxRetry   = kingpin.Flag("maxRetry", "Retry limit").Default("5").Int()
)

func main() {
	kingpin.Parse()
	app := gin.Default()

	app.POST("/produce", func(c *gin.Context) {
		var message models.Message
		if err := c.ShouldBindJSON(&message); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if err := kafka.PushToKafka(*brokerList, *topic, *maxRetry, message); err != nil {
			log.Printf("Failed to push to Kafka: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to send message to Kafka"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"message": "Message sent to Kafka"})
	})
	app.Run(":3000")
}
