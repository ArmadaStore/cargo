package lib

import (
	"fmt"
	"os"
	"time"
)

func logTime() {
	currTime := time.Now()
	fmt.Fprintf(os.Stderr, "%s", currTime.Format("2021-01-02 13:01:02 :: "))
}
