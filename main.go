package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/sqs"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/seniorescobar/bolha/client"
	"github.com/seniorescobar/bolha/lambda/common"

	log "github.com/sirupsen/logrus"
)

const (
	// TODO get queue by name
	qURL = "https://sqs.eu-central-1.amazonaws.com/301808156345/bolha-ads-queue"
)

func Handler(ctx context.Context, event events.SQSEvent) error {
	sess := session.Must(session.NewSession())

	// init aws service clients
	var (
		sqsc = sqs.New(sess)
		ddbc = dynamodb.New(sess)
	)

	for _, record := range event.Records {
		var action, username, password string
		getMessageAttributes(record.MessageAttributes, map[string]*string{
			"action":   &action,
			"username": &username,
			"password": &password,
		})

		// TODO add session id option
		c, err := client.New(&client.User{
			Username: username,
			Password: password,
		})
		if err != nil {
			return err
		}

		switch action {
		case "upload":
			var ad common.Ad
			if err := json.Unmarshal([]byte(record.Body), &ad); err != nil {
				return err
			}

			s3Client := common.NewS3Client(sess)

			// TODO add concurrency
			images := make([]io.Reader, len(ad.Images))
			for i, imgPath := range ad.Images {
				img, err := s3Client.DownloadImage(imgPath)
				if err != nil {
					return err
				}

				images[i] = img
			}

			uploadedAdId, err := c.UploadAd(&client.Ad{
				Title:       ad.Title,
				Description: ad.Description,
				Price:       ad.Price,
				CategoryId:  ad.CategoryId,
				Images:      images,
			})
			if err != nil {
				return err
			}

			input := &dynamodb.UpdateItemInput{
				ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
					":uploadedId": {
						N: aws.String(strconv.FormatInt(uploadedAdId, 10)),
					},
					":uploadedAt": {
						S: aws.String(time.Now().Format(time.RFC3339)),
					},
				},
				Key: map[string]*dynamodb.AttributeValue{
					"AdTitle": {
						S: aws.String(ad.Title),
					},
				},
				TableName:        aws.String("Bolha"),
				UpdateExpression: aws.String("SET AdUploadedId = :uploadedId, AdUploadedAt = :uploadedAt"),
			}

			if _, err := ddbc.UpdateItem(input); err != nil {
				return err
			}

			deleteSQSMessage(sqsc, record.ReceiptHandle)

		case "remove":
			uploadedAdId, err := strconv.ParseInt(record.Body, 10, 64)
			if err != nil {
				return err
			}

			if err := c.RemoveAd(uploadedAdId); err != nil {
				return err
			}

			// if err := pdb.RemoveUploadedAd(ctx, uploadedAdId); err != nil {
			// 	return err
			// }

			deleteSQSMessage(sqsc, record.ReceiptHandle)
		}
	}

	return nil
}

func getMessageAttributes(msga map[string]events.SQSMessageAttribute, pairs map[string]*string) error {
	for key, val := range pairs {
		m, ok := msga[key]
		if !ok {
			return fmt.Errorf(`missing message attributes "%s"`, key)
		}

		*val = *m.StringValue
	}

	return nil
}

func deleteSQSMessage(sqsc *sqs.SQS, receiptHandle string) error {
	log.WithField("receiptHandle", receiptHandle).Info("deleting message from sqs queue")

	_, err := sqsc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(qURL),
		ReceiptHandle: aws.String(receiptHandle),
	})

	return err
}

func main() {
	lambda.Start(Handler)
}
