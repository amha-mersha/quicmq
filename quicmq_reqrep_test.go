package quicmq

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
)

func TestReqRep(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	rep := NewRep(ctx)
	defer rep.Close()

	if err := rep.Listen("quic://127.0.0.1:0"); err != nil {
		t.Fatalf("rep.Listen: %v", err)
	}
	endpoint := fmt.Sprintf("quic://%s", rep.Addr().String())

	req1 := NewReq(ctx)
	defer req1.Close()

	grp, gctx := errgroup.WithContext(ctx)

	var (
		reqName = NewMsgString("NAME")
		reqLang = NewMsgString("LANG")
		reqQuit = NewMsgString("QUIT")
		repName = NewMsgString("quicmq")
		repLang = NewMsgString("Go")
		repQuit = NewMsgString("bye")
	)

	grp.Go(func() error {
		loop := true
		for loop {
			msg, err := rep.Recv()
			if err != nil {
				return fmt.Errorf("could not recv REQ message: %w", err)
			}
			var reply Msg
			switch string(msg.Frames[0]) {
			case "NAME":
				reply = repName
			case "LANG":
				reply = repLang
			case "QUIT":
				reply = repQuit
				loop = false
			}

			err = rep.Send(reply)
			if err != nil {
				return fmt.Errorf("could not send REP message: %w", err)
			}
		}
		return nil
	})

	grp.Go(func() error {
		err := req1.Dial(endpoint)
		if err != nil {
			return fmt.Errorf("could not dial: %w", err)
		}

		for _, msg := range []struct {
			req Msg
			rep Msg
		}{
			{reqName, repName},
			{reqLang, repLang},
			{reqQuit, repQuit},
		} {
			err = req1.Send(msg.req)
			if err != nil {
				return fmt.Errorf("could not send REQ message %v: %w", msg.req, err)
			}
			reply, err := req1.Recv()
			if err != nil {
				return fmt.Errorf("could not recv REP message %v: %w", msg.req, err)
			}

			if got, want := reply.Frames, msg.rep.Frames; !reflect.DeepEqual(got, want) {
				return fmt.Errorf("got = %v, want= %v", got, want)
			}
		}
		return nil
	})

	if err := grp.Wait(); err != nil {
		t.Fatalf("error: %+v", err)
	}
	_ = gctx
}

func TestReqStateEnforcement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := NewReq(ctx)
	defer req.Close()

	// Initial state: Send is allowed, Recv is not.
	_, err := req.Recv()
	if err == nil {
		t.Fatalf("expected error when calling Recv before Send, got nil")
	}

	// Wait for a connection to be available before sending
	rep := NewRep(ctx)
	defer rep.Close()
	_ = rep.Listen("quic://127.0.0.1:0")
	_ = req.Dial(fmt.Sprintf("quic://%s", rep.Addr().String()))
	time.Sleep(100 * time.Millisecond)

	err = req.Send(NewMsgString("test"))
	if err != nil {
		t.Fatalf("unexpected error on Send: %v", err)
	}

	// State: Send is blocked until Recv.
	err = req.Send(NewMsgString("test2"))
	if err == nil {
		t.Fatalf("expected error when calling Send twice consecutively, got nil")
	}
}

func TestMultiReqRep(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	rep := NewRep(ctx)
	defer rep.Close()

	if err := rep.Listen("quic://127.0.0.1:0"); err != nil {
		t.Fatalf("rep.Listen: %v", err)
	}
	endpoint := fmt.Sprintf("quic://%s", rep.Addr().String())

	var wg sync.WaitGroup

	// Replier
	wg.Go(func() {
		for range 4 {
			msg, err := rep.Recv()
			if err != nil {
				t.Errorf("could not recv REQ message: %v", err)
				return
			}
			data := string(msg.Frames[0])
			replyData := "reply to " + data
			err = rep.Send(NewMsgString(replyData))
			if err != nil {
				t.Errorf("could not send REP message: %v", err)
				return
			}
		}
	})

	// Requesters
	for i := range 2 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			req := NewReq(ctx)
			defer req.Close()

			if err := req.Dial(endpoint); err != nil {
				t.Errorf("req[%d].Dial: %v", idx, err)
				return
			}
			time.Sleep(100 * time.Millisecond)

			for j := range 2 {
				reqData := fmt.Sprintf("req%d-msg%d", idx, j)
				if err := req.Send(NewMsgString(reqData)); err != nil {
					t.Errorf("req[%d].Send: %v", idx, err)
					return
				}
				reply, err := req.Recv()
				if err != nil {
					t.Errorf("req[%d].Recv: %v", idx, err)
					return
				}
				expected := "reply to " + reqData
				if got := string(reply.Frames[0]); got != expected {
					t.Errorf("req[%d] got %q, want %q", idx, got, expected)
				}
			}
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("Timeout waiting for goroutines")
	}
}
