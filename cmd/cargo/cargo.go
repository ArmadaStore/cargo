// cargo entry point

package main

import (
	"os"

	"github.com/ArmadaStore/cargo/pkg/cmd"
	"github.com/ArmadaStore/cargo/pkg/cmd/cargo"
)

func main() {
	// cargo manager IP and Port information passed in as cmd line arguments
	cargoMgrIP := os.Args[1]
	cargoMgrPort := os.Args[2]
	cargoPort := os.Args[3]
	volSize := os.Args[4]

	// Start execution of cargo instance
	err := cargo.Run(cargoMgrIP, cargoMgrPort, cargoPort, volSize)
	cmd.CheckError(err)
}
