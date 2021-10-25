package main

import (
	"context"
	"github.com/farit2000/hdfs_parquet_generator/internal/consumer"
	"github.com/farit2000/hdfs_parquet_generator/internal/pkg"
	"log"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	var hdfsConf pkg.HDFSConf
	hdfsConf.GetHDFSConf()
	hdfsClient := hdfsConf.GetHDFSClient()

	var rmqConf pkg.RMQConf
	rmqConf.GetRMQConf()
	rmqConn, rmqChannel := rmqConf.GetRMQConnAndChannel()

	defer func() {
		stop()
		hdfsClient.Close()
		rmqConn.Close()
	}()

	c := consumer.Consumer{
		HdfsClient: hdfsClient,
		RmqConn:    rmqConn,
		RmqChannel: rmqChannel,
	}

	err := c.Consume(ctx)
	pkg.FailOnError(err, "Consumer error")

	log.Println("Consumer exiting")
}
