package main

import (
	"bufio"
	"fmt"
	"github.com/streadway/amqp"
	"os"
)

func importQueue(host string, port int, queueName string, fileName string, consume bool) error {
	fmt.Println(fmt.Sprintf("Importing queue %s into file %s", queueName, fileName))
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	connection, err := amqp.Dial(fmt.Sprintf("amqp://%s:%d", host, port))
	if err != nil {
		return err
	}
	defer connection.Close()

	channel, err := connection.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	counter := 0
	for {
		msg, ok, err := channel.Get(queueName, false)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		fmt.Fprintln(writer, string(msg.Body))
		if consume {
			msg.Ack(false)
		}
		if counter++; counter%10000 == 0 {
			fmt.Println(fmt.Sprintf("Messages imported: %d", counter))
		}
	}
	fmt.Println(fmt.Sprintf("Done, %d messages imported", counter))
	return nil
}
