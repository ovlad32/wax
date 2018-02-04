package appnode


type WorkerInterface interface {
	Id() string
	Close() error
}

func (node *ApplicationNodeType) AppendWorker(a WorkerInterface) {
	if node.agents == nil {
		node.slavesMux.Lock()
		if node.agents == nil {
			node.agents = make(map[string]WorkerInterface)
		}
		node.agents[a.Id()] = a
		node.slavesMux.Unlock()
	} else {
		node.slavesMux.Lock()
		node.agents[a.Id()] = a
		node.slavesMux.Unlock()
	}
}
func (node *ApplicationNodeType) FindWorkerById(id string) WorkerInterface {
	node.slavesMux.RLock()
	agent, found := node.agents[id]
	node.slavesMux.RUnlock()
	if found {
		return agent
	}
	return nil
}

func (node *ApplicationNodeType) RemoveWorkerById(id string) {
	node.slavesMux.Lock()
	delete(node.agents, id)
	node.slavesMux.Unlock()
}

func (node *ApplicationNodeType) CloseRegularWorker(
	sourceSubject, replySubject string,
	message *CommandMessageType) (err error) {

	id := message.ParmString("workerSubject", "")
	if id == "" {

	}

	agent := node.FindWorkerById(id)
	if agent == nil {

	}

	err = agent.Close()
	if err != nil {

	}
	resp := &CommandMessageType{
		Command: CATEGORY_SPLIT_CLOSED,
	}
	err = node.enc.Publish(replySubject, resp)

	err = node.enc.Flush()
	if err = node.enc.LastError(); err != nil {

	}

	node.RemoveWorkerById(id)
	return
}

