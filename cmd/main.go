package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"s3_conversion/secrets"
	"strings"
	"sync"
	"syscall"
	"time"
)

const newSuffix = "___delimited.gz"

func main() {

	startTime := time.Now()

	// Create a channel to receive OS signals
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	// Create a channel to signal shutdown
	shutdown := make(chan struct{})

	// Start a go routine that will close the shutdown channel when an OS signal is received
	go func() {
		<-sig
		close(shutdown)
	}()

	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				secrets.AwsAccessKey,
				secrets.AwsSecretKey,
				"")))
	if err != nil {
		panic(err)
	}

	client := s3.NewFromConfig(cfg)

	sourceKeys := make(chan string, 10_000) // Add a buffer size to avoid blocking
	SourceKeysCount := 0

	var targetKeys = make(map[string]bool)

	input := &s3.ListObjectsV2Input{
		Bucket: &secrets.AwsBucket,
		Prefix: &secrets.AwsBucketPrefix,
	}

	paginator := s3.NewListObjectsV2Paginator(client, input)

	for paginator.HasMorePages() {
		output, err := paginator.NextPage(context.Background())
		if err != nil {
			log.Fatalf("failed to get objects, %v", err)
		}

		for _, obj := range output.Contents {
			key := *obj.Key
			if !strings.Contains(key, newSuffix) {
				sourceKeys <- key
				SourceKeysCount++
			} else {
				targetKeys[key] = true
			}
		}
	}
	close(sourceKeys) // Close the channel here, after all keys have been added

	if err != nil {
		panic(err)
	}

	log.Printf("Total number of items to process: %d\n", SourceKeysCount)
	var wg sync.WaitGroup

	// Count processed items
	processedItems := 0
	processedItemsMutex := &sync.Mutex{}

	// Start worker goroutines
	for i := 0; i < secrets.MaxConcurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for key := range sourceKeys { // This will loop until keyCh is closed
				targetKey := strings.Replace(key, ".gz", newSuffix, 1)
				select {
				case <-shutdown:
					log.Println(fmt.Sprintf("[%d]: Shutdown signal received, stopping...", workerID))
					return // Terminate this goroutine
				default:
					if !targetKeys[targetKey] {
						processFile(client, key, targetKey, workerID+1)
						processedItemsMutex.Lock()
						processedItems++
						processedItemsMutex.Unlock()
					} else {
						// If the delimited version already exists, delete the original file
						log.Println("Delimited file already exists. Deleting original file:", key)
						deleteObj := &s3.DeleteObjectInput{
							Bucket: &secrets.AwsBucket,
							Key:    &key,
						}

						_, err := client.DeleteObject(context.Background(), deleteObj)
						if err != nil {
							panic(err)
						}
					}
				}
			}
		}(i) // Pass the worker ID to the goroutine
	}
	wg.Wait()
	log.Printf("Total number of items processed: %d\n", processedItems)
	totalProcessTime := time.Since(startTime)
	log.Printf("Processed all keys in %v", totalProcessTime)

	totalMinutes := totalProcessTime.Minutes()
	itemsPerMinute := float64(processedItems) / totalMinutes
	log.Printf("Processed %v items per minute", itemsPerMinute)

}

func closeBody(Body io.ReadCloser) {
	err := Body.Close()
	if err != nil {
		panic(err)
	}
}

func closeReader(gr *gzip.Reader) {
	err := gr.Close()
	if err != nil {
		panic(err)
	}
}

func processFile(client *s3.Client, key string, targetKey string, workerID int) {
	sourceFileName := filepath.Base(key)
	targetFileName := filepath.Base(targetKey)
	log.Printf("[%d] Downloading file: [%s]", workerID, sourceFileName)
	input := &s3.GetObjectInput{
		Bucket: &secrets.AwsBucket,
		Key:    &key,
	}

	res, err := client.GetObject(context.Background(), input)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer closeBody(res.Body)

	gr, err := gzip.NewReader(res.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer closeReader(gr)

	buf := new(bytes.Buffer)
	data, err := io.ReadAll(gr)
	if err != nil {
		panic(err)
	}
	buf.Write(data)

	log.Printf("[%d] Replacing file: [%s]", workerID, sourceFileName)
	newStr := strings.Replace(buf.String(), "}{", "}\n{", -1)

	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	if _, err := gz.Write([]byte(newStr)); err != nil {
		fmt.Println(err)
		return
	}
	if err := gz.Close(); err != nil {
		panic(err)
	}

	log.Printf("[%d] Uploading file: [%s]", workerID, targetFileName)
	inputPut := &s3.PutObjectInput{
		Bucket: &secrets.AwsBucket,
		Key:    &targetKey,
		Body:   bytes.NewReader(b.Bytes()),
	}
	_, err = client.PutObject(context.Background(), inputPut)
	if err != nil {
		fmt.Println(err)
		return
	}

	log.Printf("[%d] Deleting file: [%s]", workerID, sourceFileName)
	deleteObj := &s3.DeleteObjectInput{
		Bucket: &secrets.AwsBucket,
		Key:    &key,
	}

	_, err = client.DeleteObject(context.Background(), deleteObj)
	if err != nil {
		panic(err)
	}

}
