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

package ste

import (
	"fmt"
	"io"
	"net/url"
	"os"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/handlers"
	"github.com/Azure/azure-storage-file-go/2017-04-17/azfile"
)

// this struct is created for each transfer
type fileToLocal struct {
	transfer         *TransferMsg
	fileURL          azfile.FileURL
	memoryMappedFile handlers.MMap
	srcFileHandler   *os.File
	ranges           []string
}

// return a new fileToLocal struct targeting a specific transfer
func newFileToLocal(transfer *TransferMsg, pacer *pacer) xfer {
	// download is not paced
	return &fileToLocal{transfer: transfer}
}

func (fileToLocal *fileToLocal) runPrologue(chunkChannel chan<- ChunkMsg) {
	// step 1: create fileUrl for source file
	p := azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{
		Retry: azfile.RetryOptions{
			Policy:        azfile.RetryPolicyExponential,
			MaxTries:      DownloadMaxTries,
			TryTimeout:    DownloadTryTimeout,
			RetryDelay:    DownloadRetryDelay,
			MaxRetryDelay: DownloadMaxRetryDelay,
		},
		Log: pipeline.LogOptions{
			Log: func(l pipeline.LogLevel, msg string) {
				fileToLocal.transfer.Log(common.LogLevel(l), msg)
			},
			MinimumLevelToLog: func() pipeline.LogLevel {
				return pipeline.LogLevel(fileToLocal.transfer.MinimumLogLevel)
			},
		},
	})
	u, _ := url.Parse(fileToLocal.transfer.Source)
	fileToLocal.fileURL = azfile.NewFileURL(*u, p)

	// step 2: get size info for the download
	fileSize := int64(fileToLocal.transfer.SourceSize)
	downloadChunkSize := int64(fileToLocal.transfer.BlockSize)

	// step 3a: short-circuit if source has no content
	if fileSize == 0 {
		fmt.Println("Got here for destination", fileToLocal.transfer.Destination)
		executionEngineHelper{}.createEmptyFile(fileToLocal.transfer.Destination)
		fileToLocal.preserveLastModifiedTimeOfSourceToDestination()

		// run epilogue early
		fileToLocal.transfer.Log(common.LogInfo, " concluding the download Transfer of job after creating an empty file")
		fileToLocal.transfer.TransferStatus(common.TransferComplete)
		fileToLocal.transfer.TransferDone()

	} else { // 3b: source has content

		// step 4: prep local file before download starts
		fileToLocal.memoryMappedFile, fileToLocal.srcFileHandler = executionEngineHelper{}.createAndMemoryMapFile(fileToLocal.transfer.Destination, fileSize)

		// step 5: go through the file range and schedule download chunk jobs
		blockIdCount := int32(0)
		for startIndex := int64(0); startIndex < fileSize; startIndex += downloadChunkSize {
			adjustedChunkSize := downloadChunkSize

			// compute exact size of the chunk
			if startIndex+downloadChunkSize > fileSize {
				adjustedChunkSize = fileSize - startIndex
			}

			// schedule the download chunk job
			chunkChannel <- ChunkMsg{
				doTransfer: fileToLocal.generateDownloadFunc(
					blockIdCount, // serves as index of chunk
					adjustedChunkSize,
					startIndex),
			}
			blockIdCount += 1
		}
	}
}

// this generates a function which performs the downloading of a single chunk
func (fileToLocal *fileToLocal) generateDownloadFunc(chunkId int32, adjustedChunkSize int64, startIndex int64) chunkFunc {
	return func(workerId int) {
		totalNumOfChunks := uint32(fileToLocal.transfer.NumChunks)

		if fileToLocal.transfer.TransferContext.Err() != nil {
			if fileToLocal.transfer.ChunksDone() == totalNumOfChunks {
				fileToLocal.transfer.Log(common.LogInfo,
					fmt.Sprintf(" has worker %d which is finalizing cancellation of the Transfer", workerId))
				fileToLocal.transfer.TransferDone()
			}
		} else {
			// step 1: perform get
			get, err := fileToLocal.fileURL.Download(fileToLocal.transfer.TransferContext, startIndex, adjustedChunkSize, false)
			if err != nil {
				// cancel entire transfer because this chunk has failed
				// TODO consider encapsulating cancel operation on transferMsg
				fileToLocal.transfer.TransferCancelFunc()
				fileToLocal.transfer.Log(common.LogInfo, fmt.Sprintf(" has worker %d which is canceling job and chunkId %d because startIndex of %d has failed", workerId, chunkId, startIndex))
				fileToLocal.transfer.TransferStatus(common.TransferFailed)
				if fileToLocal.transfer.ChunksDone() == totalNumOfChunks {
					fileToLocal.transfer.Log(common.LogInfo,
						fmt.Sprintf(" has worker %d which finalizing cancellation of Transfer", workerId))
					fileToLocal.transfer.TransferDone()
				}
				return
			}
			resilientReader := get.Body(azfile.ResilientReaderOptions{MaxRetryRequests: DownloadMaxTries})

			// step 2: write the body into the memory mapped file directly
			bytesRead, err := io.ReadFull(resilientReader, fileToLocal.memoryMappedFile[startIndex:startIndex+adjustedChunkSize])
			resilientReader.Close()
			if int64(bytesRead) != adjustedChunkSize || err != nil {
				// cancel entire transfer because this chunk has failed
				fileToLocal.transfer.TransferCancelFunc()
				fileToLocal.transfer.Log(common.LogInfo, fmt.Sprintf(" has worker %d is canceling job and chunkId %d because writing to file for startIndex of %d has failed", workerId, chunkId, startIndex))
				fileToLocal.transfer.TransferStatus(common.TransferFailed)
				if fileToLocal.transfer.ChunksDone() == totalNumOfChunks {
					fileToLocal.transfer.Log(common.LogInfo,
						fmt.Sprintf(" has worker %d is finalizing cancellation of Transfer", workerId))
					fileToLocal.transfer.TransferDone()
				}
				return
			}

			fileToLocal.transfer.jobInfo.JobThroughPut.updateCurrentBytes(adjustedChunkSize)

			// step 3: check if this is the last chunk
			if fileToLocal.transfer.ChunksDone() == totalNumOfChunks {
				// step 4: this is the last block, perform EPILOGUE
				fileToLocal.transfer.Log(common.LogInfo,
					fmt.Sprintf(" has worker %d which is concluding download Transfer of job after processing chunkId %d", workerId, chunkId))
				fileToLocal.transfer.TransferStatus(common.TransferComplete)
				fileToLocal.transfer.TransferDone() //TODO rename to MarkTransferAsDone? ChunksDone also uses Done but return a number instead of updating status

				fileToLocal.memoryMappedFile.Unmap()
				err := fileToLocal.srcFileHandler.Close()
				if err != nil {
					fileToLocal.transfer.Log(common.LogError,
						fmt.Sprintf(" has worker %v which failed to close the file %s and failed with error %s", workerId, fileToLocal.srcFileHandler.Name(), err.Error()))
				}

				fileToLocal.preserveLastModifiedTimeOfSourceToDestination()
			}
		}
	}
}

func (fileToLocal *fileToLocal) preserveLastModifiedTimeOfSourceToDestination() {
	// TODO this doesn't seem to be working (on Mac), the date is set to Wednesday, December 31, 1969 at 4:00 PM for some reason
	// TODO need more investigation + tests
	lastModifiedTime, preserveLastModifiedTime := fileToLocal.transfer.PreserveLastModifiedTime()
	if preserveLastModifiedTime {
		err := os.Chtimes(fileToLocal.transfer.Destination, lastModifiedTime, lastModifiedTime)
		if err != nil {
			fileToLocal.transfer.Log(common.LogError, fmt.Sprintf(" not able to preserve last modified time for destionation %s", fileToLocal.transfer.Destination))
			return
		}
		fileToLocal.transfer.Log(common.LogInfo, fmt.Sprintf("successfully preserve the last modified time for destinaton %s", fileToLocal.transfer.Destination))
	}
}
