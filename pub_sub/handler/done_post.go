package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"pub_sub/components"
	"pub_sub/kafka"

	"github.com/gin-gonic/gin"
)

func DonePost() gin.HandlerFunc {
	return func(c *gin.Context) {

		var body components.Request_body
		c.BindJSON(&body)
		e, err := json.Marshal(body)
		fmt.Println(err)

		c.JSON(200, gin.H{
			"status": "Transaction Successful",
		})
		err1 := kafka.Push(context.Background(), nil, []byte(e))
		fmt.Println(err1)

	}
}
