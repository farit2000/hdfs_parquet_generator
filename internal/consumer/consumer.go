package consumer

import (
	"bytes"
	"context"
	"fmt"
	"github.com/colinmarc/hdfs"
	"github.com/farit2000/hdfs_parquet_generator/internal/pkg"
	"github.com/streadway/amqp"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"log"
)

type Consumer struct {
	RmqConn    *amqp.Connection
	RmqChannel *amqp.Channel
	HdfsClient *hdfs.Client
}

func writeParquetFile(buff *bytes.Buffer, pw *writer.ParquetWriter, hdfsClient *hdfs.Client, fileNum int) {
	err := pw.WriteStop()
	pkg.FailOnError(err, "Error while write stop parquet writer")
	w, err := hdfsClient.Create(fmt.Sprintf("/user/hive/warehouse/parquets/randJson%d.parquet", fileNum))
	defer func() {
		w.Close()
		buff.Reset()
	}()

	pkg.FailOnError(err, "Error while create parquet file")
	count, err := w.Write(buff.Bytes())
	log.Printf("Writed %d", count)
}

func readFromRabbitAndSendToParquetFileHDFS(ctx context.Context, rmqChannel *amqp.Channel, hdfsClient *hdfs.Client) {
	q, err := rmqChannel.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	pkg.FailOnError(err, "Failed to declare a queue")

	msgs, err := rmqChannel.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	pkg.FailOnError(err, "Failed to register a consumer")

	var buff bytes.Buffer

	pw, err := writer.NewParquetWriterFromWriter(&buff, new(pkg.RandomJsonStruct), 4)
	pkg.FailOnError(err, "Error while create new parquet writer")
	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	var jsonStruct pkg.RandomJsonStruct
	i := 0
	fileNum := 0
	go func() {
		for d := range msgs {
			err = pw.Write(jsonStruct.Unmarshal(d.Body))
			pkg.FailOnError(err, "Error while writing to file")
			log.Printf("Received a message: %d", jsonStruct.Unmarshal(d.Body).Id)
			i++
			log.Println(i)
			if i == 50000 {
				writeParquetFile(&buff, pw, hdfsClient, fileNum)
				fileNum++
				i = 0
			}
		}
	}()

	<-ctx.Done()
	writeParquetFile(&buff, pw, hdfsClient, fileNum)
	log.Println("Consume stop")
}

func (consumer *Consumer) Consume(ctx context.Context) error {
	cancelContext, cancel := context.WithCancel(ctx)
	defer cancel()

	readFromRabbitAndSendToParquetFileHDFS(cancelContext, consumer.RmqChannel, consumer.HdfsClient)
	<-ctx.Done()
	log.Println("Consume complete!")
	return nil
}
