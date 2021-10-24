package pkg

import (
	json2 "encoding/json"
	"math/rand"
)

type RandomJsonStruct struct {
	Id        int    `json:"id" parquet:"name=id, type=INT32"`
	FirstName string `json:"first_name" parquet:"name=first_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	LastName  string `json:"last_name" parquet:"name=last_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	City      string `json:"city" parquet:"name=city, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Zip       int    `json:"zip" parquet:"name=zip, type=INT32"`
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func (jsonStruct *RandomJsonStruct) GenerateNew() *RandomJsonStruct {
	jsonStruct.Id = rand.Intn(10000)
	jsonStruct.FirstName = randString(15)
	jsonStruct.LastName = randString(15)
	jsonStruct.City = randString(10)
	jsonStruct.Zip = rand.Intn(999999)
	return jsonStruct
}

func (jsonStruct *RandomJsonStruct) Unmarshal(jsonData []byte) *RandomJsonStruct {
	err := json2.Unmarshal(jsonData, jsonStruct)
	FailOnError(err, "error while unmarshal json")
	return jsonStruct
}

func (jsonStruct *RandomJsonStruct) Marshal() []byte {
	jsonData, err := json2.Marshal(jsonStruct)
	FailOnError(err, "Error while marshal json")
	return jsonData
}
