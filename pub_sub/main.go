package main

import (
	"pub_sub/handler"
	"pub_sub/kafka"

	"github.com/gin-gonic/gin"
)

func main() {

	router := gin.Default()
	kafka.RunConnection()

	router.POST("/api/v1/produce", handler.DonePost())

	router.Run(":8080")

}
