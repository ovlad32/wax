package appnode

import (
	"github.com/gorilla/mux"
	"fmt"
	"time"
	"net/http"
	"github.com/pkg/errors"
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



func (node masterApplicationNodeType) copyFileHandlerFunc() func (http.ResponseWriter,*http.Request) {
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

		sourceNodeId:= r.FormValue("sourceNodeId")

		if !found || sourceNodeId == "" {
			err := errors.New("sourceNodeId is empty")
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
		targetNodeId:= r.FormValue("targetNodeId")
		if !found || targetNodeId == "" {
			err := errors.New("targetNodeId is empty")
			node.logger.Error(err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		subj,err := node.copyfile(sourceNodeId,sourceFile,targetNodeId,targetFile);
		node.logger.Info(subj);
		if err!= nil {
			node.logger.Error(err);
		}
		if err != nil {

		}

		fmt.Fprint(w,"ok")


		return
	}
}
func (node masterApplicationNodeType) copyfile(
	srcNodeId,
	srcFile,
	dstNodeId,
	dstFile string,
) (subject string,err error) {

	sourceNodeId := NodeIdType(srcNodeId)
	if sourceNodeId.IsEmpty() {
		//TODO:
	}

	destinationNodeId := NodeIdType(dstNodeId)
	if sourceNodeId.IsEmpty() {
		//TODO:
	}


	statsResp,err := node.RequestCommandBySubject(
		sourceNodeId.CommandSubject(),
		fileStats,
		NewCommandMessageParams(1).
			Append(filePathParam,srcFile)...
	)
	if statsResp == nil {
		err = errors.Errorf("response from source node is not initialized")
		node.logger.Error(err)
		return
	}
	if statsResp.Command != fileStats {
		err  = errors.Wrapf(err,"response from source node is not recognized: %v",statsResp.Command)
		node.logger.Error(err)
		return
	}
	if statsResp.Err != "" {
		err  = errors.Errorf("response from source node with error: %v",statsResp.Err)
		node.logger.Error(err)
		return
	}
	fileNotExists := statsResp.ParamBool(fileNotExistsParam,false)
	if fileNotExists {
		err  = errors.Errorf("source file does not exist: %v",srcFile)
		node.logger.Error(err)
		return
	}
	fileSize := statsResp.ParamInt64(fileSizeParam,-1)
	if fileSize == -1{
		err  = errors.Errorf("fileSizeParam parameter is absent")
		node.logger.Error(err)
		return
	} else if fileSize == 0 {
		err  = errors.Errorf("source file is empty: %v",srcFile)
		node.logger.Error(err)
		return
	}



	subsResp,err := node.RequestCommandBySubject(
		destinationNodeId.CommandSubject(),
		copyFileDataSubscribe,
		NewCommandMessageParams(3).
			Append(filePathParam,dstFile).
			Append(fileSizeParam,fileSize)...
	)
/*&CommandMessageParamEntryType{
			Key:filePathParam,
			Value:dstFile,
		},
		&CommandMessageParamEntryType{
			Key:fileSizeParam,
			Value:fileSize,
		},
		&CommandMessageParamEntryType{
			Key:slaveCommandSubjectParam,
			Value:sourceNodeId.CommandSubject(),
		},*/
	if subsResp == nil {

		return
	}
	if subsResp.Command != copyFileDataSubscribe {

		return
	}

	if subsResp.Err != ""{

		return
	}
	dataSubject := subsResp.ParamSubject(workerSubjectParam)

	err = node.PublishCommand(
		sourceNodeId.CommandSubject(),
		copyFileLaunch,
		NewCommandMessageParams(1).
			Append(filePathParam,srcFile).
			Append(workerSubjectParam,dataSubject)...
	)

	return dataSubject.String(),nil
}

func (node masterApplicationNodeType) copyfile1(
	sourceNodeId,
	sourceFile,
	targetNode,
	targetFile string,
	) (subject string,err error)  {
/*
	//subs := node.commandSubject(targetNode)
	entry,found :=  node.slaveCommandSubjects[targetNode]
	if !found {
		err= errors.Errorf("entry !found")
		node.logger.Error(err)
		return
	}
	if entry.subject == "" {
		err= errors.Errorf("entry.subject== nil ")
		node.logger.Error(err)
		return
	}

	fileStats,err := os.Stat(sourceFile)
	if err != nil {
		err = errors.Wrapf(err,"could not get source file information: %v",sourceFile)
		node.logger.Error(err)
		return
	}
	if fileStats.Size() == 0 {
		err = errors.Errorf("Source file is empty: %v",sourceFile)
		node.logger.Error(err)
		return
	}

	file, err := os.Open(sourceFile)
	if err != nil {
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
	ctx,entry.cancelFunc := context.WithCancel(context.Background())

	go func () {

		buf := bufio.NewReader(file)


		{
			emptyMsg := &CommandMessageType{
				Command: copyFileData,
				Params: CommandMessageParamMap{
					copyFileDataParam: make([]byte,0),
					copyFileEOFParam:  true,
				},
			}
			b, err := node.encodedConn.Enc.Encode(subject,emptyMsg)
			fmt.Println(int64(len(b)),len(b),node.encodedConn.Conn.MaxPayload(),err)
		}

		var readBuffer []byte
		var dataSizeAdjusted = false
		var maxPayloadSize = node.encodedConn.Conn.MaxPayload()

		if dataSizeAdjusted = fileStats.Size() < maxPayloadSize/2; dataSizeAdjusted {
			node.logger.Warnf("readBuffer allocated to the size of the sourceFile %v",fileStats.Size())
			readBuffer = make([]byte, fileStats.Size())
		} else {
			var adjustment int64
			adjustment, dataSizeAdjusted = node.payloadSizeAdjustments[copyFileData]
			readBuffer = make([]byte, maxPayloadSize - adjustment)
		}


		for {
			var eof bool
			readBytes, err := buf.Read(readBuffer);
			if err != nil {
				if err != io.EOF {
					//todo:
					node.logger.Error(err)
					return
				} else {
					eof = true
				}
			}

			replica := make([]byte, readBytes)
			copy(replica, readBuffer[:readBytes])

			message := &CommandMessageType{
				Command: copyFileData,
				Params: CommandMessageParamMap{
					copyFileDataParam: replica,
					copyFileEOFParam:  eof,
				},
			}
			if !dataSizeAdjusted {
				var adjustment int64
				adjustment,err  = node.registerMaxPayloadSize(message)
				if err != nil {
					//todo:
				}
				newSize := maxPayloadSize + adjustment;
				cutMessage := &CommandMessageType{
					Command: copyFileData,
					Params: CommandMessageParamMap{
						copyFileDataParam: replica[0:newSize],
						copyFileEOFParam:  eof,
					},
				}
				if err = node.encodedConn.Publish(subject, cutMessage); err != nil {
					err = errors.Wrapf(err, "could not publish copyfile cutMessage")
					node.logger.Error(err)

				}
				replica = replica[newSize:]
				readBuffer = readBuffer[0:newSize]
				node.logger.Warnf("readBuffer has been cut to %v bytes",newSize)

				dataSizeAdjusted = true
				message = &CommandMessageType{
					Command: copyFileData,
					Params: CommandMessageParamMap{
						copyFileDataParam: replica,
						copyFileEOFParam:  eof,
					},
				}
			}

			if err = node.encodedConn.Publish(subject, message); err != nil {
				err = errors.Wrapf(err, "could not publish copyfile message")
				node.logger.Error(err)

			}



			if err = node.encodedConn.Flush(); err != nil {
				err = errors.Wrapf(err,"could not flush copyfile message")
				node.logger.Error(err)
				return
			}

			if err = node.encodedConn.LastError(); err != nil {
				err = errors.Wrapf(err,"error while wiring copyfile message")
				node.logger.Error(err)
				return
			}
			if eof {
				file.Close()
				return
			}

		}
	} ()*/
	return
}




func (node *masterApplicationNodeType) initRestApiRouting() (srv *http.Server, err error){
	r := mux.NewRouter()
	r.HandleFunc("/table/index",node.BitsetBuildingHandlerFunc()).Methods("POST")
	r.HandleFunc("/table/categorysplit",node.CategorySplitHandlerFunc()).Methods("POST")
	r.HandleFunc("/util/copyfile",node.copyFileHandlerFunc()).Methods("POST")

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
