package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastMessage struct {
	Type    string
	Message int
}

func main() {
	n := maelstrom.NewNode()

	var seen []int

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var m BroadcastMessage
		if err := json.Unmarshal(msg.Body, &m); err != nil {
			return err
		}

		seen = append(seen, m.Message)

		return n.Reply(msg, map[string]string{"type": "broadcast_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{"type": "read_ok", "messages": seen})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]string{"type": "topology_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
