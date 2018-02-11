package rest

import (
	"github.com/sirupsen/logrus"
	"net/http"
)

type IndexingRequestType struct {
}

func BitsetBuildingHandler(w http.ResponseWriter, r *http.Request) {
	logrus.Info("Hello from there !")
}
