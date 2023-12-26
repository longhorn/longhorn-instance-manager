package util

import (
	commonNs "github.com/longhorn/go-common-libs/ns"
	commonTypes "github.com/longhorn/go-common-libs/types"
)

// NewExecutor creates a new namespaced executor
func NewExecutor(hostProc string) (*commonNs.Executor, error) {
	namespaces := []commonTypes.Namespace{commonTypes.NamespaceMnt, commonTypes.NamespaceIpc, commonTypes.NamespaceNet}

	return commonNs.NewNamespaceExecutor(commonTypes.ProcessNone, hostProc, namespaces)
}
