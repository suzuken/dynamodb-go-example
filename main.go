package main

import (
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"log"
)

var (
	table  = flag.String("table", "your-table-name", "your stream name")
	region = flag.String("region", "ap-northeast-1", "your AWS region")
)

// Item is sample table for table
type Item struct {
	ID       string `json:"id"`
	Field1   string `json:"field1",omitempty`
	IntField int    `json:"intfield",omitempty`
}

func main() {
	if err := realmain(); err != nil {
		log.Fatal(err)
	}
}

func realmain() error {
	flag.Parse()
	s := session.New(&aws.Config{Region: aws.String(*region)})
	ddb := dynamodb.New(s)

	tablename := aws.String(*table)

	describeTableOutput, err := ddb.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: tablename,
	})
	if err != nil {
		return err
	}
	fmt.Printf("%#v\n", describeTableOutput)

	// if table not exists, create one.
	if describeTableOutput == nil {
		// create table should specify primary key.
		// on DynamoDB, not primary key fields are not necessary when creating table.
		createTableOutput, err := ddb.CreateTable(&dynamodb.CreateTableInput{
			AttributeDefinitions: []*dynamodb.AttributeDefinition{
				{
					AttributeName: aws.String("id"),
					AttributeType: aws.String("S"),
				},
			},
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String("id"),
					KeyType:       aws.String("HASH"),
				},
			},
			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(10),
				WriteCapacityUnits: aws.Int64(10),
			},
			TableName: tablename,
		})
		if err != nil {
			return err
		}
		fmt.Printf("%#v\n", createTableOutput)
	}

	item, err := dynamodbattribute.ConvertToMap(Item{"id1", "field1", 1})
	if err != nil {
		return err
	}
	fmt.Printf("%#v\n", item)

	// PutItem request overwrite existed item (same primary key) by default.
	putItemOutput, err := ddb.PutItem(&dynamodb.PutItemInput{
		Item:      item,
		TableName: tablename,
	})
	if err != nil {
		return err
	}
	// empty response even if successfly put
	fmt.Printf("%#v\n", putItemOutput)

	getItemOutput, err := ddb.GetItem(&dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String("id1"),
			},
		},
		TableName: tablename,
	})
	if err != nil {
		return err
	}
	fmt.Printf("%#v\n", getItemOutput)

	var putreq []*dynamodb.WriteRequest
	for i := 0; i < 10; i++ {
		it, err := dynamodbattribute.ConvertToMap(Item{
			ID:       fmt.Sprintf("batchwrite_id%d", i+100),
			Field1:   fmt.Sprintf("batchwriteItem%d", i),
			IntField: i,
		})
		if err != nil {
			return err
		}
		putreq = append(putreq, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: it,
			},
		})
	}

	batchWriteItemOutput, err := ddb.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			*table: putreq,
		},
	})
	if err != nil {
		return err
	}
	fmt.Printf("%#v\n", batchWriteItemOutput)

	batchGetItemOutput, err := ddb.BatchGetItem(&dynamodb.BatchGetItemInput{
		RequestItems: map[string]*dynamodb.KeysAndAttributes{
			*table: {
				Keys: []map[string]*dynamodb.AttributeValue{
					{
						"id": {
							S: aws.String("batchwrite_id100"),
						},
					},
					{
						"id": {
							S: aws.String("batchwrite_id101"),
						},
					},
				},
			},
		},
	})
	if err != nil {
		return err
	}
	fmt.Printf("%#v\n", batchGetItemOutput)

	deleteTableOutput, err := ddb.DeleteTable(&dynamodb.DeleteTableInput{
		TableName: tablename,
	})
	if err != nil {
		return err
	}
	fmt.Printf("%#v", deleteTableOutput)

	return nil
}
