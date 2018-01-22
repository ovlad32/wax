package rest

import (
	"net/http"
	"github.com/sirupsen/logrus"
)

type IndexingRequestType struct {
}

func BitsetBuildingHandler(w http.ResponseWriter, r *http.Request) {
	logrus.Info("Hello from there !")
}
