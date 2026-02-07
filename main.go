package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli/v3"
)

var rootCmd = &cli.Command{
	Name: "bpfstream",
	Commands: []*cli.Command{
		vfsCmd,
	},
}

func main() {
	log.Logger = zerolog.New(newPtermWriter()).With().Timestamp().Caller().Stack().Logger()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	err := rootCmd.Run(ctx, os.Args)
	if err != nil {
		log.Error().Err(err).Msg("Unexpected error")
	}
}
