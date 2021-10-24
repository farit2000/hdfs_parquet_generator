package pkg

import (
	"fmt"
	"github.com/colinmarc/hdfs"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

type HDFSConf struct {
	Server struct {
		Host string `yaml:"host"`
		Port string `yaml:"port"`
	} `yaml:"server"`
}

func (conf *HDFSConf) GetHDFSConf() *HDFSConf {
	yamlFile, err := ioutil.ReadFile("/home/farit/univer/datalab/hdfs_parquet_generator/configs/hdfs.yaml")
	if err != nil {
		log.Printf("yamlFile.Get err   #%v ", err)
	}
	err = yaml.Unmarshal(yamlFile, conf)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}

	return conf
}

func (conf *HDFSConf) GetHDFSClient() *hdfs.Client {
	hdfsClient, err := hdfs.New(fmt.Sprintf("%s:%s", conf.Server.Host, conf.Server.Port))
	FailOnError(err, "Failed to connect to HDFS")
	return hdfsClient
}
