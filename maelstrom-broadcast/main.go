package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/hashicorp/go-set/v2"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastMessage struct {
	Type    string
	Message int
}

type TopologyMessage struct {
	Type     string
	Topology map[string][]string
}

type GossipMessage struct {
	Type     string
	Messages []int
}

type State struct {
	mutex sync.Mutex
	seen  set.Set[int]
	known map[string]set.Set[int]
}

func main() {
	n := maelstrom.NewNode()

	var neighbors []string
	state := State{seen: *set.New[int](10), known: make(map[string]set.Set[int])}

	// Gossip every 500 milliseconds
	ticker := time.NewTicker(700 * time.Millisecond)
	go func() {
		for {
			<-ticker.C // block until we get a tick

			state.mutex.Lock() // Aquire a lock on node state

			for _, neighbor := range neighbors {
				known_to := state.known[neighbor]

				// send all messages that we know about that the neighbor doesn't
				var to_notify []int
				state.seen.ForEach(func(m int) bool {
					if !known_to.Contains(m) {
						to_notify = append(to_notify, m)
					}
					return true
				})

				if to_notify != nil {
					n.Send(neighbor, GossipMessage{Type: "gossip", Messages: to_notify})
				}
			}

			state.mutex.Unlock()
		}
	}()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var m BroadcastMessage
		if err := json.Unmarshal(msg.Body, &m); err != nil {
			return err
		}

		// Aquire a lock on node state
		state.mutex.Lock()
		defer state.mutex.Unlock()

		state.seen.Insert(m.Message)

		return n.Reply(msg, map[string]string{"type": "broadcast_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		// Aquire a lock on node state
		state.mutex.Lock()
		defer state.mutex.Unlock()

		return n.Reply(msg, map[string]any{"type": "read_ok", "messages": state.seen.Slice()})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var m TopologyMessage
		if err := json.Unmarshal(msg.Body, &m); err != nil {
			return err
		}

		neighbors = m.Topology[n.ID()]

		return n.Reply(msg, map[string]string{"type": "topology_ok"})
	})

	n.Handle("gossip", func(msg maelstrom.Message) error {
		var m GossipMessage
		if err := json.Unmarshal(msg.Body, &m); err != nil {
			return err
		}

		// Aquire a lock on node state
		state.mutex.Lock()
		defer state.mutex.Unlock()

		// we have now seen these messages...
		state.seen.InsertSlice(m.Messages)
		// ...but we also know that the node that just gossiped to us knows about them
		know_to, ok := state.known[msg.Src]
		if !ok {
			know_to = *set.New[int](10)
			state.known[msg.Src] = know_to
		}
		know_to.InsertSlice(m.Messages)

		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
