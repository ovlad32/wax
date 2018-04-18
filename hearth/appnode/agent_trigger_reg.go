package appnode

func registerAgentTriggers(n *node) {
	n.commandTriggers[agentTerminate] = onAgentTerminate(n)
	n.commandTriggers[fileStats] = onActionRun(n)
}
