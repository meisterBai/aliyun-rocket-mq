package alirmq

import (
	"fmt"
	"testing"
)

func TestNewOnsClientTopic(t *testing.T) {
	onsClient, err := NewOnsClient("", "", "", "")
	if err != nil {
		fmt.Printf("error = %v\n", err)
		return
	}

	rows, err := onsClient.ListTopic()
	if err != nil {
		fmt.Printf("error = %v\n", err)
		return
	}

	for _, row := range rows {
		fmt.Printf("row = %v\n", *row)
	}

	//err = onsClient.CreateTopic("topic", "test", NormalMessageType)
	//if err != nil {
	//	fmt.Printf("error = %v\n", err)
	//	return
	//}

	topics := []string{
		"topic",
		"FromApiB",
		"FromApi",
	}
	for _, topic := range topics {
		err = onsClient.DeleteTopic(topic)
		if err != nil {
			fmt.Printf("error = %v\n", err)
		}
	}
}

func TestNewOnsClientGroup(t *testing.T) {
	onsClient, err := NewOnsClient("", "", "", "")
	if err != nil {
		fmt.Printf("error = %v\n", err)
		return
	}

	//rows, err := onsClient.ListGroup()
	//for _, row := range rows {
	//	fmt.Printf("row = %v\n", row)
	//}

	//err = onsClient.CreateGroup("GID_C_ApiTest", "GID_C_ApiTest")
	//if err != nil {
	//	t.Errorf("error = %v\n", err)
	//}
	err = onsClient.DeleteGroup("GID_C_ApiTest")
	if err != nil {
		t.Errorf("error = %v", err)
	}
}

func TestNewOnsClientConsumer(t *testing.T) {
	onsClient, err := NewOnsClient("", "", "", "")
	if err != nil {
		fmt.Printf("error = %v\n", err)
		return
	}

	err = onsClient.ClearConsumerTopicMessage("GID_C-CommonUserStudent02", "CommonUserStudent")
	if err != nil {
		t.Errorf("error = %v", err)
	}
}