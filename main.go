package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/hld3/event-common-go/events"
	"github.com/hld3/event-send-messages-go/sender"
)

var s *sender.RabbitMQSender

func main() {

	var err error
	s, err = sender.NewSender()
	if err != nil {
		log.Fatalf("Failed to initialize sender: %v", err)
	}

	r := mux.NewRouter()
	r.HandleFunc("/userData", sendUserDataEvent)
	log.Fatal(http.ListenAndServe(":8080", r))
	defer s.Close()

	//Why doesn't this show? ListenAndServe?
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
}

func sendUserDataEvent(w http.ResponseWriter, r *http.Request) {
	var message events.UserDataEvent
	err := json.NewDecoder(r.Body).Decode(&message)
	if err != nil {
		log.Println("Error parsing the request:", err)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Error parsing the request\n"))
		return
	}
	sendMessage(message, "UserDataEvent")
	w.WriteHeader(http.StatusAccepted)
}

func sendMessage(messageR events.UserDataEvent, eventType string) error {
	//TODO continue the update here.
	message := fmt.Sprintf("{\"nodeId\": \"%s\", \"userId\": \"%s\", \"username\": \"%s\", \"status\": \"%s\", \"comment\": \"%s\", \"receiveUpdates\": \"%v\"}",
		messageR.NodeId, messageR.UserId, messageR.Username, messageR.Status, messageR.Comment, messageR.ReceiveUpdates)
	if err := s.SendMessage(message, eventType); err != nil {
		return errors.New(fmt.Sprintf("Failed to send message: %v", err))
	}
	log.Printf("Sending message: %s", message)
	return nil
}
