// publicly availble code for cargo entry

package cargo

import (
	"fmt"
	"sync"

	"github.com/ArmadaStore/cargo/pkg/lib"
)

func Run(cargoMgrIP string, cargoMgrPort string, cargoPort string, volSize string) error {
	cargoInfo := lib.Init(cargoMgrIP, cargoMgrPort, cargoPort, volSize)
	cargoInfo.Register()

	var wg sync.WaitGroup
	wg.Add(1)
	go cargoInfo.ListenTasks(&wg)

	// wg.Add(1)
	// go cargoInfo.SendToReplicas()

	wg.Wait()
	return fmt.Errorf("Hello")
}
