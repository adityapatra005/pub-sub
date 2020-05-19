package main

import (
	"context"
	"encoding/json"
	"fmt"
	"pub_sub/handler"
	"pub_sub/kafka"

	"github.com/gin-gonic/gin"
)

/*func runProducer() {

	err := kafka.Push(context.Background(), nil, []byte("Transaction Done"))
	fmt.Println(err)
}*/

func runConnection() {

	kafkaBrokersUrls := []string{"localhost:19092", "localhost:29092", "localhost:39092"}
	var clientId string = "first_consumer"
	var foo string = "foo"
	fmt.Println(kafkaBrokersUrls)
	var w, error = kafka.Configure(kafkaBrokersUrls, clientId, foo)

	fmt.Println(w, error)

}

func main() {

	router := gin.Default()
	runConnection()
	router.POST("/done", func(c *gin.Context) {

		var body handler.Request_body
		c.BindJSON(&body)
		e, err := json.Marshal(body)
		fmt.Println(err)

		if body.Message_body == "successfull" {

			c.JSON(200, gin.H{
				"status": "Transaction Successful",
			})
			err := kafka.Push(context.Background(), nil, []byte(e))
			fmt.Println(err)

		} else {
			c.JSON(200, gin.H{
				"Status": "Your transaction failed",
			})
		}

	})

	router.Run(":8080")

}
