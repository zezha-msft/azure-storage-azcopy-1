package ste

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"unsafe"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/handlers"
	"github.com/Azure/azure-storage-file-go/2017-04-17/azfile"
)

type localToFile struct {
	transfer         *TransferMsg
	pacer            *pacer
	fileURL          azfile.FileURL
	memoryMappedFile handlers.MMap
	srcFileHandler   *os.File
}

// return a new localToFile struct targeting a specific transfer
func newLocalToFile(transfer *TransferMsg, pacer *pacer) xfer {
	return &localToFile{transfer: transfer, pacer: pacer}
}

func (localToFile *localToFile) isUserEndpointStyle(url url.URL) bool {
	pathStylePorts := map[int16]struct{}{10000: struct{}{}, 10001: struct{}{}, 10002: struct{}{}, 10003: struct{}{}, 10004: struct{}{}, 10100: struct{}{}, 10101: struct{}{}, 10102: struct{}{}, 10103: struct{}{}, 10104: struct{}{}, 11000: struct{}{}, 11001: struct{}{}, 11002: struct{}{}, 11003: struct{}{}, 11004: struct{}{}, 11100: struct{}{}, 11101: struct{}{}, 11102: struct{}{}, 11103: struct{}{}, 11104: struct{}{}}

	// Decides whether it's user endpoint style, and compose the new path.
	if net.ParseIP(url.Host) != nil {
		return true
	}

	if url.Port() != "" {
		port, err := strconv.Atoi(url.Port())
		if err != nil {
			return false
		}
		if _, ok := pathStylePorts[int16(port)]; ok {
			return true
		}
	}

	return false
}

func (localToFile *localToFile) getServiceBaseAddress(u url.URL, p pipeline.Pipeline) azfile.ServiceURL {
	path := u.Path

	if path != "" {
		if path[0] == '/' {
			path = path[1:]
		}
		if localToFile.isUserEndpointStyle(u) {
			u.Path = path[:strings.Index(path, "/")]
		} else {
			u.Path = ""
		}
	}

	return azfile.NewServiceURL(u, p)
}

// TODO: jiac consider to make it a method in File client: ParentDir for file.
func (localToFile *localToFile) getParentDir(fileURL azfile.FileURL, p pipeline.Pipeline) azfile.DirectoryURL {
	u := fileURL.URL()
	u.Path = u.Path[:strings.LastIndex(u.Path, "/")]
	return azfile.NewDirectoryURL(u, p)
}

func verifyAndHandleCreateErrors(err error) error {
	if err != nil {
		sErr := err.(azfile.StorageError)
		if sErr != nil &&
			(sErr.Response().StatusCode == http.StatusNotFound ||
				sErr.Response().StatusCode == http.StatusForbidden ||
				sErr.Response().StatusCode == http.StatusConflict) { // Note the ServiceCode actually be AuthenticationFailure when share failed to be created.
			return nil
		}
		return err
	}

	return nil
}

func splitWithoutToken(str string, token rune) []string {
	return strings.FieldsFunc(str, func(c rune) bool {
		return c == token
	})
}

// createParentDirToRoot creates parent directories of the file, if file's parent directory doesn't exist.
func (localToFile *localToFile) createParentDirToRoot(dirURL azfile.DirectoryURL, p pipeline.Pipeline) error {

	ctx := localToFile.transfer.TransferContext

	segment := splitWithoutToken(dirURL.URL().Path, '/')
	// if it's user endpoint style, ignore the first segment, which would be the account name.
	if localToFile.isUserEndpointStyle(dirURL.URL()) {
		segment = segment[1:]
	}

	_, err := dirURL.GetProperties(ctx)
	if err != nil {
		if err.(azfile.StorageError) != nil &&
			(err.(azfile.StorageError).Response().StatusCode == http.StatusNotFound ||
				err.(azfile.StorageError).Response().StatusCode == http.StatusForbidden) { // Might be lack of read permission
			// fileParentDirURL doesn't exist, try to create the directory and share to the root.
			// try to create the share
			serviceURL := localToFile.getServiceBaseAddress(dirURL.URL(), p)
			fmt.Println(serviceURL)
			shareURL := serviceURL.NewShareURL(segment[0])
			_, err := shareURL.Create(ctx, azfile.Metadata{}, 0)
			if verifiedErr := verifyAndHandleCreateErrors(err); verifiedErr != nil {
				return verifiedErr
			}

			curDirURL := shareURL.NewRootDirectoryURL()
			// try to create the directories
			for i := 1; i < len(segment); i++ {
				curDirURL = curDirURL.NewDirectoryURL(segment[i])
				fmt.Println(curDirURL)
				_, err := curDirURL.Create(ctx, azfile.Metadata{})
				if verifiedErr := verifyAndHandleCreateErrors(err); verifiedErr != nil {
					return verifiedErr
				}
			}
		} else {
			return err
		}
	}

	return nil
}

// this function performs the setup for each transfer and schedules the corresponding upload range requests into the chunkChannel
func (localToFile *localToFile) runPrologue(chunkChannel chan<- ChunkMsg) {

	// step 1: create pipeline for the destination file
	p := azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{
		Retry: azfile.RetryOptions{
			Policy:        azfile.RetryPolicyExponential,
			MaxTries:      UploadMaxTries,
			TryTimeout:    UploadTryTimeout,
			RetryDelay:    UploadRetryDelay,
			MaxRetryDelay: UploadMaxRetryDelay,
		},
		Log: pipeline.LogOptions{
			Log: func(l pipeline.LogLevel, msg string) {
				localToFile.transfer.Log(common.LogLevel(l), msg)
			},
			MinimumLevelToLog: func() pipeline.LogLevel {
				return pipeline.LogLevel(localToFile.transfer.MinimumLogLevel)
			},
		},
	})

	u, _ := url.Parse(localToFile.transfer.Destination)
	localToFile.fileURL = azfile.NewFileURL(*u, p)
	fmt.Println(localToFile.fileURL)

	// step 2: map in the file to upload before appending files
	localToFile.memoryMappedFile, localToFile.srcFileHandler = executionEngineHelper{}.openAndMemoryMapFile(localToFile.transfer.Source)
	fileHTTPHeaders, metaData := localToFile.transfer.fileHttpHeaderAndMetadata(localToFile.memoryMappedFile)

	// step 3a: Create the parent directories or share for the file
	err := localToFile.createParentDirToRoot(localToFile.getParentDir(localToFile.fileURL, p), p)
	if err != nil {
		localToFile.transfer.Log(common.LogError, fmt.Sprintf("failed since Create parent directory for file failed due to %s", err.Error()))
		localToFile.cleanupForPrologueError()
		return
	}

	// step 3b: Create or resize file of the source size
	fileSize := int64(localToFile.transfer.SourceSize)
	_, err = localToFile.fileURL.Create(localToFile.transfer.TransferContext, fileSize, fileHTTPHeaders, metaData)
	// TODO: jiac Add overwrite logic?
	if err != nil && err.(azfile.StorageError) != nil && err.(azfile.StorageError).ServiceCode() == azfile.ServiceCodeResourceAlreadyExists {
		_, err = localToFile.fileURL.Resize(localToFile.transfer.TransferContext, fileSize)
	}
	if err != nil {
		localToFile.transfer.Log(common.LogError, fmt.Sprintf("failed since Create file failed due to %s", err.Error()))
		localToFile.cleanupForPrologueError()
		return
	}

	chunkSize := int64(localToFile.transfer.BlockSize)

	// step 4: Scheduling (page) range update to the object created in Step 3
	for startIndex := int64(0); startIndex < fileSize; startIndex += chunkSize {
		adjustedChunkSize := chunkSize

		// compute actual size of the chunk
		if startIndex+chunkSize > fileSize {
			adjustedChunkSize = fileSize - startIndex
		}

		// schedule the chunk job/msg
		chunkChannel <- ChunkMsg{
			doTransfer: localToFile.generateUploadFunc(
				uint32(localToFile.transfer.NumChunks),
				startIndex,
				adjustedChunkSize),
		}
	}
}

func (localToFile *localToFile) cleanupForPrologueError() {
	localToFile.transfer.TransferCancelFunc()
	localToFile.transfer.TransferDone()
	localToFile.transfer.TransferStatus(common.TransferFailed)
	localToFile.memoryMappedFile.Unmap()
	err := localToFile.srcFileHandler.Close()
	if err != nil {
		localToFile.transfer.Log(common.LogError, fmt.Sprintf("got an error while closing file %s because of %s", localToFile.srcFileHandler.Name(), err.Error()))
	}
}

func (localToFile *localToFile) generateUploadFunc(numberOfRanges uint32, startOffset int64, chunkSize int64) chunkFunc {
	return func(workerId int) {
		t := localToFile.transfer
		file := localToFile.srcFileHandler
		// chunk done is the function called after success / failure of each chunk.
		// If the calling chunk is the last chunk of transfer, then it updates the transfer status,
		// mark transfer done, unmap the source memory map and close the source file descriptor.
		rangeDone := func(status common.TransferStatus) {
			if t.ChunksDone() == numberOfRanges {
				// Transfer status
				if status != common.TransferInProgress {
					t.TransferStatus(status)
				}
				t.Log(common.LogInfo,
					fmt.Sprintf("has worker %d which is finalizing transfer", workerId))
				t.TransferDone()
				localToFile.memoryMappedFile.Unmap()
				err := file.Close()
				if err != nil {
					t.Log(common.LogError, fmt.Sprintf("got an error while closing file %s because of %s", file.Name(), err.Error()))
				}
			}
		}

		if t.TransferContext.Err() != nil {
			t.Log(common.LogInfo, fmt.Sprintf("is cancelled. Hence not picking up range %d", startOffset))
			rangeDone(common.TransferInProgress)
		} else {
			// rangeBytes is the byte slice of file for the given range
			rangeBytes := localToFile.memoryMappedFile[startOffset : startOffset+chunkSize]
			// converted the bytes slice to int64 array.
			// converting each of 8 bytes of byteSlice to an integer.
			int64Slice := (*(*[]int64)(unsafe.Pointer(&rangeBytes)))[:len(rangeBytes)/8]

			allBytesZero := true
			// Iterating though each integer of in64 array to check if any of the number is greater than 0 or not.
			// If any no is greater than 0, it means that the 8 bytes slice represented by that integer has atleast one byte greater than 0
			// If all integers are 0, it means that the 8 bytes slice represented by each integer has no byte greater than 0
			for index := 0; index < len(int64Slice); index++ {
				if int64Slice[index] > 0 {
					// If one number is greater than 0, then we need to perform the UploadRange update.
					allBytesZero = false
					break
				}
			}

			// If all the bytes in the rangeBytes is 0, then we do not need to perform the UploadRange
			// Updating number of chunks done.
			if allBytesZero {
				t.Log(common.LogInfo, fmt.Sprintf("has worker %d which is not performing UploadRange for Range range from %d to %d since all the bytes are zero", workerId, startOffset, startOffset+chunkSize))
				rangeDone(common.TransferComplete)
				return
			}

			body := newRequestBodyPacer(bytes.NewReader(rangeBytes), localToFile.pacer)

			_, err := localToFile.fileURL.UploadRange(t.TransferContext, startOffset, body)
			if err != nil {
				status := common.TransferInProgress
				if t.TransferContext.Err() != nil {
					t.Log(common.LogError,
						fmt.Sprintf("has worker %d which failed to upload range from %d to %d because transfer was cancelled", workerId, startOffset, startOffset+chunkSize))
				} else {
					t.Log(common.LogError,
						fmt.Sprintf("has worker %d which failed to upload range from %d to %d because of following error %s", workerId, startOffset, startOffset+chunkSize, err.Error()))
					// cancelling the transfer
					t.TransferCancelFunc()
					status = common.TransferFailed
				}
				rangeDone(status)
				return
			}
			t.Log(common.LogInfo, fmt.Sprintf("has workedId %d which successfully complete PUT range request from range %d to %d", workerId, startOffset, startOffset+chunkSize))

			//updating the through put counter of the Job
			t.jobInfo.JobThroughPut.updateCurrentBytes(int64(chunkSize))
			// this check is to cover the scenario when the last range is successfully updated, but transfer was cancelled.
			if t.TransferContext.Err() != nil {
				rangeDone(common.TransferInProgress)
			} else {
				rangeDone(common.TransferComplete)
			}
		}
	}
}
