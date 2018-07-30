package main

import (
	"flag"
	"fmt"
	"os"
)

const (
	IMPORT = "import"
	EXPORT = "export"
)

func main() {
	mode := flag.String("mode", "", "import/export")
	queueName := flag.String("queue-name", "", "queue name")
	exchangeName := flag.String("exchange-name", "", "exchange name")
	fileName := flag.String("file-name", "", "file name")
	host := flag.String("host", "", "rabbitmq host")
	port := flag.Int("port", 5672, "rabbitmq port")
	consume := flag.Bool("consume", false, "whether messages need to be consumed or left in the queue")
	count := flag.Int("count", 0, "number of messages to import/export")
	tick := flag.Int("tick", 10000, "messages displayed in logs every [tick] messages imported/exported")

	flag.Parse()

	if *mode == "" ||
		(*mode == IMPORT && *queueName == "") ||
		(*mode == EXPORT && *exchangeName == "") ||
		*fileName == "" || *host == "" || *port == 0 {
		fmt.Println("Missing parameter(s)")
		os.Exit(1)
	}

	switch *mode {
	case IMPORT:
		if err := importQueue(*host, *port, *queueName, *fileName, *consume, *count, *tick); err != nil {
			fmt.Println(fmt.Sprintf("Error importing: %s", err.Error()))
		}
	case EXPORT:
		if err := exportQueue(*host, *port, *exchangeName, *fileName, *count, *tick); err != nil {
			fmt.Println(fmt.Sprintf("Error exporting: %s", err.Error()))
		}
	default:
		fmt.Println(fmt.Sprintf("Mode not supported: %s", *mode))
		os.Exit(1)
	}
}
