package main

import (
	"consumer/components"
	"encoding/json"
	"fmt"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {

	c_phone, errPhone := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:19092",
		"group.id":          "phone",
		"auto.offset.reset": "earliest",
	})
	c_mail, errMail := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:19092",
		"group.id":          "email",
		"auto.offset.reset": "earliest",
	})

	if errPhone != nil {
		panic(errPhone)
	}
	if errMail != nil {
		panic(errMail)
	}
	c_phone.SubscribeTopics([]string{"foo", "^aRegex.*[Tt]opic"}, nil)
	c_mail.SubscribeTopics([]string{"foo", "^aRegex.*[Tt]opic"}, nil)

	for {
		msgPhone, errP := c_phone.ReadMessage(-1)
		msgMail, errM := c_mail.ReadMessage(-1)
		valuePhone := string(msgPhone.Value)
		valueMail := string(msgMail.Value)
		var rbP components.Request_body
		var rbE components.Request_body
		json.Unmarshal([]byte(valuePhone), &rbP)
		json.Unmarshal([]byte(valueMail), &rbE)

		mbP := rbP.Message_body
		tidP := rbP.Transaction_id
		mbE := rbE.Message_body
		tidE := rbE.Transaction_id
		email := rbE.Email
		mobile := rbP.Phone
		//key := rb.Key

		if errP == nil {
			fmt.Printf("Message on phone consumer %s: \n", msgPhone.TopicPartition)
			fmt.Printf("Offset:%s Transaction ID: %s, Message: %s\n  Mobile: %s\n", msgPhone.TopicPartition.Offset, tidP, mbP, mobile)

			components.SMS(mobile, tidP, mbP)
		} else {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", errP, msgPhone)
		}
		if errM == nil {
			fmt.Printf("Message on mail consumer %s: \n", msgPhone.TopicPartition)
			fmt.Printf("Offset:%s Transaction ID: %s, Message: %s\n Email: %s\n ", msgPhone.TopicPartition.Offset, tidP, mbP, email)
			components.Mail(email, tidE, mbE)
		} else {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", errM, msgMail)
		}
	}
	//c.Close()
}
