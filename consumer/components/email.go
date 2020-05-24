package components

import (
	"fmt"
	"net/smtp"
)

// smtpServer data to smtp server
type smtpServer struct {
	host string
	port string
}

// Address URI to smtp server
func (s *smtpServer) Address() string {
	return s.host + ":" + s.port
}
func Mail(email string, tid string, msg string) {
	// Sender data.
	from := "adityapdummy@gmail.com"
	password := "XXXX"

	// Receiver email address.
	to := []string{
		email,
	}

	// smtp server configuration.
	smtpServer := smtpServer{host: "smtp.gmail.com", port: "587"}
	// Message.
	message := []byte(msg + "\n Your transaction ID is:" + tid + "\n")
	// Authentication.
	auth := smtp.PlainAuth("", from, password, smtpServer.host)
	// Sending email.
	err := smtp.SendMail(smtpServer.Address(), auth, from, to, message)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Email Sent!")
}
