package stores

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type OSSInfo struct {
	URL        string
	AccessKey  string
	SecretKey  string
	BucketName string
	CanWrite   bool
}

type StorageOSSInfo = OSSInfo

type OSSClient struct {
	s3Client  *s3.S3
	s3Session *session.Session
	s3Info    OSSInfo
}

func NewOSSClient(info StorageOSSInfo) (*OSSClient, error) {
	sess, err := session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(info.AccessKey, info.SecretKey, ""),
		Endpoint:         aws.String(info.URL),
		Region:           aws.String("us-west-2"),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(false),
	})

	if err != nil {
		return nil, err
	}

	cli := s3.New(sess)
	buckets, err := cli.ListBuckets(nil)

	if err != nil {
		return nil, err
	}

	bucketExists := false
	for _, bucket := range buckets.Buckets {
		if *bucket.Name == info.BucketName {
			bucketExists = true
			break
		}
	}

	ossCli := &OSSClient{
		s3Client:  cli,
		s3Session: sess,
		s3Info:    info,
	}
	if !bucketExists {
		err = ossCli.CreateBucket()
		if err != nil {
			return nil, err
		}
	}

	return ossCli, nil
}

func (oss *OSSClient) CreateBucket() error {
	_, err := oss.s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(oss.s3Info.BucketName),
	})
	if err != nil {
		return err
	}

	err = oss.s3Client.WaitUntilBucketExists(&s3.HeadBucketInput{
		Bucket: aws.String(oss.s3Info.BucketName),
	})
	if err != nil {
		return err
	}

	return nil
}
