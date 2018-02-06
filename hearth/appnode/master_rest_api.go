package appnode

import (
	"github.com/gorilla/mux"
	"fmt"
	"time"
	"net/http"
	"github.com/pkg/errors"
)
type handler func (http.ResponseWriter,*http.Request)

func (node *masterApplicationNodeType) BitsetBuildingHandlerFunc() func (http.ResponseWriter,*http.Request)  {
	return func (w http.ResponseWriter,r *http.Request) {
		vars := mux.Vars(r)
		_ = vars
		//tableInfo := vars[tableInfoIdParam.String()]
		return
	}

}

func (node *masterApplicationNodeType) CategorySplitHandlerFunc() func (http.ResponseWriter,*http.Request)  {
	return func (w http.ResponseWriter,r *http.Request) {
		vars := mux.Vars(r)
		_ = vars
		//tableInfoId := vars[tableInfoIdParam.String()]
		//tableInfoId := vars[tableInfoIdParam.String()]
		return
	}
}



func (node *masterApplicationNodeType) initRestApiRouting() (srv *http.Server, err error){
	r := mux.NewRouter()
	r.HandleFunc("/table/index",node.BitsetBuildingHandlerFunc()).Methods("POST")
	r.HandleFunc("/table/categorysplit",node.CategorySplitHandlerFunc()).Methods("POST")

	//defer node.wg.Done()
	address := fmt.Sprintf(":%d",node.config.MasterRestPort)
	srv = &http.Server{
		Handler: r,
		Addr:    address,
		// Good practice: enforce timeouts for servers you create!
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}
	node.logger.Infof("REST API server has started at %v....", address)
	go func () {
		err := srv.ListenAndServe()
		if err != nil {
			if err == http.ErrServerClosed {
				node.logger.Warn("REST API server closed")
				return
			}
			err = errors.Wrapf(err ,"REST API server broke at %v: %v", address)
			node.logger.Fatal(err)
		}
	}()

	return
}
