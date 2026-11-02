package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

)

func main() {
	
	ctx, cancel := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer cancel()

	if err := StartDispatch(ctx); err != nil {
		println("Starting dispatch...")
		log.Fatalf("dispatcher failed: %v", err)
	}
}