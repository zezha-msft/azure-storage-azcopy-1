package handlers

import (
	"errors"
	"fmt"

	"github.com/Azure/azure-storage-azcopy/common"
)

type downloadEnumerator interface {
	// Property setter and getter
	JobPartOrderToFill() *common.CopyJobPartOrderRequest
	Transfers() []common.CopyTransfer
	SetTransfers(transfer []common.CopyTransfer)
	PartNumber() int
	SetPartNumber(partNumber int)

	// Behavior
	enumerate(sourceUrlString string, isRecursiveOn bool, destinationPath string) error
}

// accept a new transfer, simply add to the list of transfers and wait for the dispatch call to send the order
func addTransfer(enumerator downloadEnumerator, transfer common.CopyTransfer) {
	enumerator.SetTransfers(append(enumerator.Transfers(), transfer))
}

// send the current list of transfer to the STE
func dispatchPart(enumerator downloadEnumerator, isFinalPart bool) error {
	// if the job is empty, throw an error
	if !isFinalPart && len(enumerator.Transfers()) == 0 {
		return errors.New("cannot initiate empty job, please make sure source is not empty")
	}

	// add the transfers and part number to template
	enumerator.JobPartOrderToFill().Transfers = enumerator.Transfers()
	enumerator.JobPartOrderToFill().PartNum = common.PartNumber(enumerator.PartNumber())

	jobStarted, errorMsg := copyHandlerUtil{}.sendJobPartOrderToSTE(enumerator.JobPartOrderToFill(), common.PartNumber(enumerator.PartNumber()), isFinalPart)
	if !jobStarted {
		return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", enumerator.JobPartOrderToFill().ID, enumerator.JobPartOrderToFill().PartNum, errorMsg)
	}

	// empty the transfers and increment part number count
	enumerator.SetTransfers([]common.CopyTransfer{})
	enumerator.SetPartNumber(enumerator.PartNumber() + 1)

	return nil
}
