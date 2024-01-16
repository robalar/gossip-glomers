package main

import (
	"context"
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type LogEntry struct {
	Offset  int
	Message int
}

type State struct {
	Logs             maelstrom.KV
	CommittedOffsets maelstrom.KV
}

func NewState(n *maelstrom.Node) State {
	return State{
		Logs: *maelstrom.NewLinKV(n), CommittedOffsets: *maelstrom.NewSeqKV(n),
	}
}

func (l *State) read_logs(key string) []LogEntry {
	var logs []LogEntry
	if err := l.Logs.ReadInto(context.TODO(), key, &logs); err != nil {
		return make([]LogEntry, 0)
	}

	return logs
}

func (l *State) append_to_log(key string, msg int) (int, error) {
	log := l.read_logs(key)

	var next_offset int
	if prev_idx := len(log) - 1; prev_idx >= 0 {
		prev := log[len(log)-1]
		next_offset = prev.Offset + 1
	}

	appended_log := append(log, LogEntry{Offset: next_offset, Message: msg})

	if err := l.Logs.CompareAndSwap(context.TODO(), key, log, appended_log, true); err != nil {
		return 0, err
	}

	return next_offset, nil
}

func (l *State) poll(key string, offset int) [][]int {
	log_entries := l.read_logs(key)

	var to_send [][]int
	for _, log_entry := range log_entries {
		if log_entry.Offset >= offset {
			to_send = append(to_send, []int{log_entry.Offset, log_entry.Message})
		}
	}

	return to_send
}

func (l *State) commit_offset(key string, offset int) error {
	// FIXME: what if offset in message are prior to the ones in state?
	return l.CommittedOffsets.Write(context.TODO(), key, offset)
}

func (l *State) read_committed_offset(key string) (int, error) {
	return l.CommittedOffsets.ReadInt(context.TODO(), key)
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

	state := NewState(n)

	n.Handle("send", func(msg maelstrom.Message) error {
		var m SendMessage
		if err := json.Unmarshal(msg.Body, &m); err != nil {
			return err
		}

		offset, err := state.append_to_log(m.Key, m.Msg)
		if err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "send_ok", "offset": offset})
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var m PollMessage
		if err := json.Unmarshal(msg.Body, &m); err != nil {
			return err
		}

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
			if err := state.commit_offset(k, v); err != nil {
				return err
			}
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
			offset, err := state.read_committed_offset(key)
			if err != nil {
				return err
			}

			offsets[key] = offset
		}

		return n.Reply(msg, map[string]any{"type": "list_committed_offsets_ok", "offsets": offsets})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
