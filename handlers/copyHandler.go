// Copyright © 2017 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package handlers

import (
	"fmt"

	"github.com/Azure/azure-storage-azcopy/common"

	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"time"
)

// handles the copy command
// dispatches the job order (in parts) to the storage engine
func HandleCopyCommand(commandLineInput common.CopyCmdArgsAndFlags) {
	jobPartOrder := common.CopyJobPartOrderRequest{}
	copyHandlerUtil{}.applyFlags(&commandLineInput, &jobPartOrder)

	// generate job id
	jobId := common.JobID(common.NewUUID())
	jobPartOrder.ID = jobId
	jobStarted := true

	// dispatch transfer
	jobStarted = dispatchTransfer(&commandLineInput, &jobPartOrder)

	// unexpected errors can happen while communicating with the transfer engine
	if !jobStarted {
		fmt.Println("Job with id", jobId, "was not able to start. Please try again")
		return
	}

	// in background mode we would spit out the job id and quit
	// in foreground mode we would continuously print out status updates for the job, so the job id is not important
	fmt.Println("Job with id", jobId, "has started.")
	if commandLineInput.IsaBackgroundOp {
		return
	}

	// created a signal channel to receive the Interrupt and Kill signal send to OS
	cancelChannel := make(chan os.Signal, 1)
	// cancelChannel will be notified when os receives os.Interrupt and os.Kill signals
	signal.Notify(cancelChannel, os.Interrupt, os.Kill)

	// waiting for signals from either cancelChannel or timeOut Channel.
	// if no signal received, will fetch/display a job status update then sleep for a bit
	for {
		select {
		case <-cancelChannel:
			fmt.Println("Cancelling Job")
			HandleCancelCommand(jobId.String())
			os.Exit(1)
		default:
			jobStatus := copyHandlerUtil{}.fetchJobStatus(jobId)

			// happy ending to the front end
			if jobStatus == "JobCompleted" {
				os.Exit(0)
			}

			// wait a bit before fetching job status again, as fetching has costs associated with it on the backend
			time.Sleep(500 * time.Millisecond)
		}
	}
	return
}

func dispatchTransfer(commandLineInput *common.CopyCmdArgsAndFlags,
	jobPartOrder *common.CopyJobPartOrderRequest) bool {
	switch {
	case commandLineInput.SourceType == common.Blob || commandLineInput.DestinationType == common.Blob:
		return handleBlobTransfer(commandLineInput, jobPartOrder)
	case commandLineInput.SourceType == common.File || commandLineInput.DestinationType == common.File:
		return handleFileTransfer(commandLineInput, jobPartOrder)
	default:
		fmt.Printf("Cannot launch tranfer from source type %v to destination type %v.", commandLineInput.SourceType, commandLineInput.DestinationType)
		return false
	}
}

func handleBlobTransfer(commandLineInput *common.CopyCmdArgsAndFlags,
	jobPartOrder *common.CopyJobPartOrderRequest) bool {

	jobStarted := false
	// not having a valid blob type is a fatal error
	if jobPartOrder.OptionalAttributes.BlobType == common.InvalidBlob {
		fmt.Println("Invalid blob type passed. Please enter the valid blob type - BlockBlob, AppendBlob, PageBlob")
		return jobStarted
	}

	// depending on the source and destination type, we process the cp command differently
	if commandLineInput.SourceType == common.Local && commandLineInput.DestinationType == common.Blob {
		jobStarted = handleUploadFromLocalToAzureStorage(commandLineInput, common.Blob, jobPartOrder)
	} else if commandLineInput.SourceType == common.Blob && commandLineInput.DestinationType == common.Local {
		jobStarted = handleDownloadFromAzureStorageToLocal(commandLineInput, common.Blob, jobPartOrder)
	}
	return jobStarted
}

func handleFileTransfer(commandLineInput *common.CopyCmdArgsAndFlags,
	jobPartOrder *common.CopyJobPartOrderRequest) bool {

	jobStarted := false
	// depending on the source and destination type, we process the cp command differently
	if commandLineInput.SourceType == common.Local && commandLineInput.DestinationType == common.File {
		jobStarted = handleUploadFromLocalToAzureStorage(commandLineInput, common.File, jobPartOrder)
	} else if commandLineInput.SourceType == common.File && commandLineInput.DestinationType == common.Local {
		jobStarted = handleDownloadFromAzureStorageToLocal(commandLineInput, common.File, jobPartOrder)
	}
	return jobStarted
}

func handleUploadFromLocalToAzureStorage(commandLineInput *common.CopyCmdArgsAndFlags,
	destType common.LocationType,
	jobPartOrderToFill *common.CopyJobPartOrderRequest) bool {

	// set the source and destination type
	jobPartOrderToFill.SourceType = common.Local
	jobPartOrderToFill.DestinationType = destType

	// attempt to parse the destination url
	destinationURL, err := url.Parse(commandLineInput.Destination)
	if err != nil {
		// the destination should have already been validated, it would be surprising if it cannot be parsed at this point
		panic(err)
	}

	// list the source files and directories
	matches, err := filepath.Glob(commandLineInput.Source)
	if err != nil || len(matches) == 0 {
		fmt.Println("Cannot find source to upload.")
		return false
	}

	enumerator := newUploadTaskEnumerator(jobPartOrderToFill)
	err = enumerator.enumerate(matches, commandLineInput.Recursive, destinationURL)

	if err != nil {
		fmt.Printf("Cannot start job due to error: %s.\n", err)
		return false
	}

	return true
}

func handleDownloadFromAzureStorageToLocal(commandLineInput *common.CopyCmdArgsAndFlags,
	srcType common.LocationType,
	jobPartOrderToFill *common.CopyJobPartOrderRequest) bool {

	// set the source and destination type
	jobPartOrderToFill.SourceType = srcType
	jobPartOrderToFill.DestinationType = common.Local

	var enumerator downloadEnumerator

	switch srcType {
	case common.Blob:
		enumerator = newDownloadBlobTaskEnumerator(jobPartOrderToFill)
	case common.File:
		enumerator = newDownloadFileTaskEnumerator(jobPartOrderToFill)
	default:
		fmt.Printf("Cannot create properly enumerator for source location type: %v", srcType)
		return false
	}

	err := enumerator.enumerate(commandLineInput.Source, commandLineInput.Recursive, commandLineInput.Destination)

	if err != nil {
		fmt.Printf("Cannot start job due to error: %s.\n", err)
		return false
	}

	return true
}
