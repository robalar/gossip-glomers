package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type TxnMessage struct {
	Txn [][3]any
}

func main() {
	n := maelstrom.NewNode()

	store := make(map[float64]float64)

	n.Handle("txn", func(msg maelstrom.Message) error {
		var m TxnMessage
		if err := json.Unmarshal(msg.Body, &m); err != nil {
			return err
		}

		for i, row := range m.Txn {
			op := row[0]
			key := row[1].(float64)

			if op == "r" {
				m.Txn[i][2] = store[key]
			} else if op == "w" {
				value := row[2].(float64)
				store[key] = value
			} else {
				log.Fatal("Unknown op", op)
			}
		}

		return n.Reply(msg, map[string]any{"type": "txn_ok", "txn": m.Txn})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
