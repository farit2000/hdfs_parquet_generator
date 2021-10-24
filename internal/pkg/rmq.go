package pkg

import (
	"fmt"
	"github.com/streadway/amqp"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

type RMQConf struct {
	Servers map[string]struct {
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
		Username string `yaml:"username"`
		Password string `yaml:"password"`
	} `yaml:"servers"`
}

func (conf *RMQConf) GetRMQConf() *RMQConf {
	yamlFile, err := ioutil.ReadFile("/home/farit/univer/datalab/hdfs_parquet_generator/configs/rmq.yaml")
	if err != nil {
		log.Printf("yamlFile.Get err   #%v ", err)
	}
	err = yaml.Unmarshal(yamlFile, conf)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}

	return conf
}

func (conf *RMQConf) GetRMQConnAndChannel() (*amqp.Connection, *amqp.Channel) {
	rmqConn, err := amqp.Dial(
		fmt.Sprintf(
			"amqp://%s:%s@%s:%s/",
			conf.Servers["rabbitmq-3"].Username, conf.Servers["rabbitmq-3"].Password,
			conf.Servers["rabbitmq-3"].Host, conf.Servers["rabbitmq-3"].Port,
		),
	)
	FailOnError(err, "Failed to connect to RabbitMQ")
	rmqChannel, err := rmqConn.Channel()
	FailOnError(err, "Failed to open a channel")
	return rmqConn, rmqChannel
}
