package main

import (
	"bufio"
	"fmt"
	"github.com/streadway/amqp"
	"os"
)

func exportQueue(host string, port int, exchangeName string, fileName string, count int, tick int) error {
	fmt.Println(fmt.Sprintf("Exporting file %s into exchange %s", fileName, exchangeName))
	file, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

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
		ok := scanner.Scan()
		if !ok {
			if scanner.Err() == nil {
				break
			}
			fmt.Println(fmt.Sprintf("Read error: %s", scanner.Err().Error()))
		} else {
			if err := channel.Publish(
				exchangeName,
				"",
				false,
				false,
				amqp.Publishing{
					ContentType:  "application/json",
					DeliveryMode: amqp.Persistent,
					Body:         scanner.Bytes(),
				}); err != nil {
				return err
			}
			if counter++; counter%tick == 0 {
				fmt.Println(fmt.Sprintf("Messages exported: %d", counter))
			}
			if count != 0 && counter >= count {
				break
			}
		}
	}
	fmt.Println(fmt.Sprintf("Done, %d messages exported", counter))
	return nil
}
