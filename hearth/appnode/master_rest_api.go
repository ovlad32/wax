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
		for k,v := range vars {
			fmt.Println(k," ",v)
		}
		found := true
		sourceFile:= r.FormValue("sourceFile")

		if !found || sourceFile == "" {
			err := errors.New("sourceFile is empty")
			node.logger.Error(err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}


		//targetFile, found := vars["targetFile"]
		targetFile:= r.FormValue("targetFile")

		if !found || targetFile == "" {
			err := errors.New("targetFile is empty")
			node.logger.Error(err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}


		//targetNode, found := vars["targetNode"]
		targetNode:= r.FormValue("targetNode")
		if !found || targetNode == "" {
			err := errors.New("targetNode is empty")
			node.logger.Error(err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		subj,err := node.copyfile(context.Background(),sourceFile,targetNode,targetFile);
		node.logger.Info(subj);
		if err!= nil {
			node.logger.Error(err);
		}

		return
	}
}

func (node masterApplicationNodeType) copyfile(ctx context.Context, sourceFile, targetNode, targetFile string) (
	subject string,err error)  {

	subs := node.commandSubject(targetNode)
	if subs == "" {
		err= errors.Errorf("subs == nil ")
		node.logger.Error(err)
		return
	}

	response := new(CommandMessageType)
	node.logger.Info(subs)
	err = node.encodedConn.Request(
		subs,
		&CommandMessageType{
			Command: copyFileOpen,
			Params: CommandMessageParamMap{
				outputFilePathParm: targetFile,
			},
		},
		response,
		nats.DefaultTimeout,
		)
	if err = node.encodedConn.Flush(); err !=nil {
		node.logger.Error(err)
		return
	}

	if err = node.encodedConn.LastError(); err!=nil {
		node.logger.Error(err)
		return
	}

	if response.Command !=copyFileOpened {
		err= errors.Errorf("response.Command !=copyFileOpened ")
		node.logger.Error(err)
		return
	}

	subject =  response.ParamString(workerSubjectParam,"")

	file, err := os.Open(sourceFile)
	if err != nil {
		node.logger.Error(err)
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
				node.logger.Error(err)
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
		if err = node.encodedConn.Publish(subject,message);err!=nil {
			node.logger.Error(err)

		}

		if err = node.encodedConn.Flush(); err !=nil {
			node.logger.Error(err)
			return
		}

		if err = node.encodedConn.LastError(); err!=nil {
			node.logger.Error(err)
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
