package appnode

func registerDispatcherTriggers(n *node) {
	n.commandTriggers[agentRegister] = onAgentRegister(n)
	n.commandTriggers[agentUnregister] = onAgentUnregister(n)
}
