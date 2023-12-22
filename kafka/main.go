package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type LogEntry struct {
	Offset  int
	Message int
}

type State struct {
	Logs            map[string][]LogEntry
	CommitedOffsets map[string]int
}

func NewState() State {
	return State{
		Logs:            make(map[string][]LogEntry),
		CommitedOffsets: make(map[string]int),
	}
}

func (l *State) append_to_log(key string, msg int) int {
	log := l.Logs[key] // TODO: what if `key` is not present?

	var next_offset int
	if prev_idx := len(log) - 1; prev_idx >= 0 {
		prev := log[len(log)-1]
		next_offset = prev.Offset + 1
	}

	l.Logs[key] = append(log, LogEntry{Offset: next_offset, Message: msg})

	return next_offset
}

func (l *State) poll(key string, offset int) [][]int {
	log_entries := l.Logs[key] // TODO: what if `key` is not present?
	log.Printf("key: %s, offset: %d, log_entries: %#v", key, offset, log_entries)
	var to_send [][]int
	for _, log_entry := range log_entries {
		if log_entry.Offset >= offset {
			to_send = append(to_send, []int{log_entry.Offset, log_entry.Message})
		}
	}

	return to_send
}

type SendMessage struct {
	Type string
	Key  string
	Msg  int
}

type PollMessage struct {
	Type    string
	Offsets map[string]int
}

type CommitOffsetsMessage struct {
	Type    string
	Offsets map[string]int
}

type ListCommittedOffsetsMessage struct {
	Type string
	Keys []string
}

func main() {
	n := maelstrom.NewNode()

	state := NewState()

	n.Handle("send", func(msg maelstrom.Message) error {
		var m SendMessage
		if err := json.Unmarshal(msg.Body, &m); err != nil {
			return err
		}

		offset := state.append_to_log(m.Key, m.Msg)
		return n.Reply(msg, map[string]any{"type": "send_ok", "offset": offset})
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var m PollMessage
		if err := json.Unmarshal(msg.Body, &m); err != nil {
			return err
		}
		log.Printf("offsets: %#v", m.Offsets)

		messages := make(map[string][][]int)
		for key, offset := range m.Offsets {
			messages[key] = state.poll(key, offset)
		}

		return n.Reply(msg, map[string]any{"type": "poll_ok", "msgs": messages})
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var m CommitOffsetsMessage
		if err := json.Unmarshal(msg.Body, &m); err != nil {
			return nil
		}

		for k, v := range m.Offsets {
			state.CommitedOffsets[k] = v // TODO: what if offset in message are prior to the ones in state?
		}

		return n.Reply(msg, map[string]string{"type": "commit_offsets_ok"})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var m ListCommittedOffsetsMessage
		if err := json.Unmarshal(msg.Body, &m); err != nil {
			return nil
		}

		offsets := make(map[string]int)
		for _, key := range m.Keys {
			offset, ok := state.CommitedOffsets[key]
			if ok {
				offsets[key] = offset
			}
		}

		return n.Reply(msg, map[string]any{"type": "list_committed_offsets_ok", "offsets": offsets})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
