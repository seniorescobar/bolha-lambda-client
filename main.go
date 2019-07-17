package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/seniorescobar/bolha/client"
	"github.com/seniorescobar/bolha/lambda/common"
)

func Handler(ctx context.Context, event events.SQSEvent) error {
	sess := session.Must(session.NewSession())

	sqsClient, err := common.NewSQSClient(sess)
	if err != nil {
		return err
	}

	ddb := dynamodb.New(sess)

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
		case common.ActionUpload:
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

			if _, err := ddb.UpdateItem(input); err != nil {
				return err
			}

			sqsClient.DeleteMessage(record.ReceiptHandle)

		case common.ActionRemove:
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

			sqsClient.DeleteMessage(record.ReceiptHandle)
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

func main() {
	lambda.Start(Handler)
}
