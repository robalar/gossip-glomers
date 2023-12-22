package main

import (
	"context"
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type AddRequest struct {
	Type  string
	Delta int
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	n.Handle("init", func(msg maelstrom.Message) error {
		// initalise this nodes counter
		return kv.Write(context.TODO(), n.ID(), 0)
	})

	n.Handle("add", func(msg maelstrom.Message) error {
		var m AddRequest
		if err := json.Unmarshal(msg.Body, &m); err != nil {
			return err
		}

		value, err := kv.ReadInt(context.TODO(), n.ID())
		if err != nil {
			return err
		}
		kv.Write(context.TODO(), n.ID(), value+m.Delta)

		return n.Reply(msg, map[string]string{"type": "add_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		// the final state of the counter is the sum of all the nodes individual counters
		total := 0
		for _, node_id := range n.NodeIDs() {
			value, err := kv.ReadInt(context.TODO(), node_id)
			// the read may fail if a node hasn't come online yet for what ever reason
			if err == nil {
				total += value
			}
		}

		return n.Reply(msg, map[string]any{"type": "read_ok", "value": total})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
