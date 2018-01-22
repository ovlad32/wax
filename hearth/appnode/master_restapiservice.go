package appnode

import (
	"github.com/gorilla/mux"
	"github.com/ovlad32/wax/hearth/rest"
	"fmt"
	"time"
	"net/http"
)

func (node *ApplicationNodeType) initRestApiRouting() {
	r := mux.NewRouter()
	r.HandleFunc("/table/index",rest.BitsetBuildingHandler).Methods("POST")
	r.HandleFunc("/table/categorysplit",rest.CategorySplitHandler).Methods("POST")

	go func () {
		defer node.wg.Done()
		address := fmt.Sprintf(":%d",node.config.RestPort)
		srv := &http.Server{
			Handler: r,
			Addr:    address,
			// Good practice: enforce timeouts for servers you create!
			WriteTimeout: 15 * time.Second,
			ReadTimeout:  15 * time.Second,
		}
		node.config.Logger.Infof("REST API server has started at %v....", address)
		err := srv.ListenAndServe()
		if err == nil {
			node.config.Logger.Fatalf("REST API server broke at %v: %v", address,err)
		}
	}()


}
