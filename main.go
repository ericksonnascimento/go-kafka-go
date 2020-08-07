// Example function-based Apache Kafka producer
package main

/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import (
	"fmt"
	"os"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {

	broker := "localhost:9092"
	topic := "Message.Communication.Sms"

	/*
		p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker,
			"ssl.ca.location":   "ca-cert",
			"security.protocol": "sasl_ssl",
			"sasl.mechanisms":   "PLAIN",
			//"ssl.endpoint.identification.algorithm": "https",
			"sasl.username": "alice",
			"sasl.password": "alice-secret"})
	*/

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker,
		"ssl.ca.location": "ca-cert"})

	if err != nil {
		fmt.Printf("Erro ao criar produtor: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Produtor %v\n", p)

	deliveryChan := make(chan kafka.Event)

	value := "Teste"
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(value),
	}, deliveryChan)

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("Falha ao escrever no topico: %v\n", m.TopicPartition.Error)
	} else {
		fmt.Printf("Mensagem produzido no topico %s [%d] offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}

	close(deliveryChan)
}
