package components

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

func SMS(phone string, tid string, msg string) {
	// Set account keys & information
	accountSid := "XXXX"
	authToken := "XXXX"
	urlStr := "https://api.twilio.com/2010-04-01/Accounts/" + accountSid + "/Messages.json"

	// Create possible message bodies
	//msg := "Hello bro!"

	// Set up rand

	// Pack up the data for our message
	msgData := url.Values{}
	msgData.Set("To", phone)
	msgData.Set("From", "+91223")
	msgData.Set("Body", tid+"\n"+msg+"\n")
	msgDataReader := *strings.NewReader(msgData.Encode())

	// Create HTTP request client
	client := &http.Client{}
	req, _ := http.NewRequest("POST", urlStr, &msgDataReader)
	req.SetBasicAuth(accountSid, authToken)
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	// Make HTTP POST request and return message SID
	resp, _ := client.Do(req)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		var data map[string]interface{}
		decoder := json.NewDecoder(resp.Body)
		err := decoder.Decode(&data)
		if err == nil {
			fmt.Println(data["sid"])
			fmt.Println("Message sent!")
		}
	} else {
		fmt.Println(resp.Status)
	}
}
