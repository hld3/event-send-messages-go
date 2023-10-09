package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
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
	r.HandleFunc("/groupData", sendGroupDataEvent)
	log.Fatal(http.ListenAndServe(":8080", r))
	defer s.Close()

	//Why doesn't this show? ListenAndServe?
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
}

func sendUserDataEvent(w http.ResponseWriter, r *http.Request) {
	var message events.BaseEvent
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

func sendGroupDataEvent(w http.ResponseWriter, r *http.Request) {
	var message events.BaseEvent
	err := json.NewDecoder(r.Body).Decode(&message)
	if err != nil {
		log.Println("Error parsing the request:", err)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Error parsing the request\n"))
		return
	}
	sendMessage(message, "GroupDataEvent")
	w.WriteHeader(http.StatusAccepted)
}

func sendMessage(messageR events.BaseEvent, eventType string) error {
	message := convertMessage(messageR, eventType)
	if err := s.SendMessage(message, eventType); err != nil {
		return errors.New(fmt.Sprintf("Failed to send message: %v", err))
	}
	log.Printf("Sending message: %s, of type: %s", message, eventType)
	return nil
}

func convertMessage(event events.BaseEvent, eventType string) string {
	var message string
	payloadI, ok := event.Payload.(map[string]interface{})

	if ok {
		var payload string
		// change to bytes in order to unmarshal in the switch.
		eventBytes, err := json.Marshal(payloadI)
		if err != nil {
			log.Fatal("Error changing payload to bytes", err)
		}

		switch eventType {
		case "UserDataEvent":
			var userEvent events.UserDataEvent
			err = json.Unmarshal(eventBytes, &userEvent)
			payload = fmt.Sprintf("{\"nodeId\": \"%s\", \"userId\": \"%s\", \"username\": \"%s\", \"status\": \"%s\", \"comment\": \"%s\", \"receiveUpdates\": %v}",
				userEvent.NodeId, userEvent.UserId, userEvent.Username, userEvent.Status, userEvent.Comment, userEvent.ReceiveUpdates)
		case "GroupDataEvent":
			var groupEvent events.GroupDataEvent
			err = json.Unmarshal(eventBytes, &groupEvent)
			payload = fmt.Sprintf("{\"name\": \"%s\", \"code\": \"%s\", \"groupId\": \"%s\", \"ownerId\": \"%s\", \"knownLanguage\": \"%s\", \"learningLanguage\": \"%s\"}",
				groupEvent.Name, groupEvent.Code, groupEvent.GroupId, groupEvent.OwnerId, groupEvent.KnownLanguage, groupEvent.LearningLanguage)
		}
		message = fmt.Sprintf("{\"messageId\": \"%s\", \"dateCode\": \"%v\", \"payload\": %s}", randomMessageId(), time.Now().UnixMilli(), payload)
	}
	return message
}

func randomMessageId() uuid.UUID {
    return uuid.New()
}
