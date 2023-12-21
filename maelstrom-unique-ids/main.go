package main

import (
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	counter := 0

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any = make(map[string]any)

		body["type"] = "generate_ok"
		body["id"] = fmt.Sprintf("%s-%d", n.ID(), counter)

		counter += 1

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
