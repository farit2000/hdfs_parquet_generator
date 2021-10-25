package producer

import (
	"context"
	"github.com/farit2000/hdfs_parquet_generator/internal/pkg"
	"github.com/streadway/amqp"
	"log"
	"sync"
	"time"
)

type Producer struct {
	RmqConn    *amqp.Connection
	RmqChannel *amqp.Channel
}

func sendToRabbitMQContext(wg *sync.WaitGroup, rmqChannel *amqp.Channel) {
	defer wg.Done()
	ch := make(chan interface{})

	go func() {
		q, err := rmqChannel.QueueDeclare(
			"hello", // name
			false,   // durable
			false,   // delete when unused
			false,   // exclusive
			false,   // no-wait
			nil,     // arguments
		)
		pkg.FailOnError(err, "Failed to declare a queue")

		var jsonStruct pkg.RandomJsonStruct
		jsonData := jsonStruct.GenerateNew().Marshal()

		err = rmqChannel.Publish(
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        jsonData,
			})
		pkg.FailOnError(err, "Failed to publish a message")
		log.Printf(" [x] Sent %s", string(jsonData))

		ch <- struct{}{}
	}()

	select {
	case <-time.After(time.Duration(250) * time.Second):
		log.Println("Force quited!")
	case <-ch:
		log.Println("Success send!")
	}
}

func (producer *Producer) Produce(ctx context.Context) error {
	forever := make(chan struct{})
	var wg sync.WaitGroup
	go func() {
		for {
			select {
			case <-ctx.Done():
				wg.Wait()
				forever <- struct{}{}
				return
			default:
				wg.Add(1)
				go sendToRabbitMQContext(&wg, producer.RmqChannel)
				time.Sleep(time.Duration(10) * time.Millisecond)
			}
		}
	}()

	<-forever
	log.Println("Produce complete!")
	return nil
}
