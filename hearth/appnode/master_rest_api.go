package appnode

import (
	"github.com/gorilla/mux"
	"fmt"
	"time"
	"net/http"
	"github.com/pkg/errors"
	"context"
	"github.com/nats-io/go-nats"
	"os"
	"bufio"
	"io"
)


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

func (node masterApplicationNodeType) copyfileHandlerFunc() func (http.ResponseWriter,*http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		_ = vars
		sourceFile, found := vars["sourceFile"]

		if found && sourceFile == "" {
			err := errors.New("sourceFile is empty")
			node.logger.Error(err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		targetNode, found := vars["targetNode"]
		if found && targetNode == "" {
			err := errors.New("targetNode is empty")
			node.logger.Error(err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		return
	}
}

func (node masterApplicationNodeType) copyfile(ctx context.Context, sourceFile, targetNode,targetFile string) (
	subject string,err error)  {

	subs := node.FindWorkerById(targetNode)
	if subs ==nil {

	}

	response := new(CommandMessageType)
	err = node.encodedConn.Request(
		subs.Subscriptions()[0].Subject,
		CommandMessageType{
			Command: copyFileOpen,
			Params: CommandMessageParamMap{
				outputFilePathParm: targetFile,
			},
		},
		response,
		nats.DefaultTimeout,
		)
	if err = node.encodedConn.Flush(); err !=nil {
		return
	}

	if err = node.encodedConn.LastError(); err!=nil {
		return
	}

	if response.Command !=copyFileOpened {

	}

	subject =  response.ParamString(workerSubjectParam,"")

	file, err := os.Open(sourceFile)
	if err != nil {
		return
	}
	buf := bufio.NewReaderSize(file,10*1024)
	for {
		var data []byte
		var eof bool
		_, err = buf.Read(data);
		if err != nil {
			if err != io.EOF {
				//todo:
				return
			} else {
				eof = true
			}
		}
		copied := make([]byte,len(data))
		copy(copied,data)

		message := CommandMessageType{
			Command:copyFileData,
			Params:CommandMessageParamMap{
				copyFileDataParam:copied,
				copyFileEOFParam: eof,
			},
		}
		err = node.encodedConn.Publish(subject,message)

		if err = node.encodedConn.Flush(); err !=nil {
			return
		}

		if err = node.encodedConn.LastError(); err!=nil {
			return
		}
		if eof {
			file.Close()
			return
		}

	}
	return
}




func (node *masterApplicationNodeType) initRestApiRouting() (srv *http.Server, err error){
	r := mux.NewRouter()
	r.HandleFunc("/table/index",node.BitsetBuildingHandlerFunc()).Methods("POST")
	r.HandleFunc("/table/categorysplit",node.CategorySplitHandlerFunc()).Methods("POST")
	r.HandleFunc("/util/copyfile",node.copyfileHandlerFunc()).Methods("POST")

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
