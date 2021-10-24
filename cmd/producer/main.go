package main

import (
	"context"
	"github.com/farit2000/hdfs_parquet_generator/internal/pkg"
	"github.com/farit2000/hdfs_parquet_generator/internal/producer"
	"log"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	var rmqConf pkg.RMQConf
	rmqConf.GetRMQConf()
	rmqConn, rmqChannel := rmqConf.GetRMQConnAndChannel()

	defer func() {
		rmqConn.Close()
		rmqChannel.Close()
		stop()
	}()

	p := producer.Producer{
		RmqConn:    rmqConn,
		RmqChannel: rmqChannel,
	}

	err := p.Produce(ctx)
	pkg.FailOnError(err, "Producer error")

	log.Println("Producer exiting")
}
