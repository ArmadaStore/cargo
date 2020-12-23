// error checks

package cmd

import (
	"context"
	"fmt"
	"os"
)

// exit by printing error id err is not nil
func CheckError(err error) {
	if err != nil {
		if err != context.Canceled {
			fmt.Fprintf(os.Stderr, "Error occured: %v\n", err)
		}
		os.Exit(1)
	}
}

// exit with the optional argument reasoning
func Exit(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
