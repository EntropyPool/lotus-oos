package stores

import (
	"fmt"
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
	Prefix     string
	CanWrite   bool
}

type StorageOSSInfo = OSSInfo

type OSSClient struct {
	s3Client    *s3.S3
	s3Session   *session.Session
	s3Info      OSSInfo
	proofBucket string
	dataBucket  string
}

type OSSObject struct {
}

func (info *OSSInfo) ProofBucket() string {
	return fmt.Sprintf("%s-%s-proof", info.BucketName, info.Prefix)
}

func (info *OSSInfo) DataBucket() string {
	return fmt.Sprintf("%s-%s-data", info.BucketName, info.Prefix)
}

func NewOSSClient(info StorageOSSInfo) (*OSSClient, error) {
	sess, err := session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(info.AccessKey, info.SecretKey, ""),
		Endpoint:         aws.String(info.URL),
		Region:           aws.String("us-west-2"),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	})

	if err != nil {
		return nil, err
	}

	cli := s3.New(sess)
	buckets, err := cli.ListBuckets(nil)

	if err != nil {
		return nil, err
	}

	log.Infof("buckets from %v", info.URL)
	log.Infof("%v", buckets)

	ossCli := &OSSClient{
		s3Client:    cli,
		s3Session:   sess,
		s3Info:      info,
		proofBucket: info.ProofBucket(),
		dataBucket:  info.DataBucket(),
	}

	bucketExists := false
	bucketName := info.ProofBucket()

	for _, bucket := range buckets.Buckets {
		if *bucket.Name == bucketName {
			bucketExists = true
			break
		}
	}

	if !bucketExists {
		err = ossCli.createBucket(ossCli.proofBucket)
		if err != nil {
			return nil, err
		}
	}

	bucketExists = false
	bucketName = info.DataBucket()

	for _, bucket := range buckets.Buckets {
		if *bucket.Name == bucketName {
			bucketExists = true
			break
		}
	}

	if !bucketExists {
		err = ossCli.createBucket(ossCli.dataBucket)
		if err != nil {
			return nil, err
		}
	}

	return ossCli, nil
}

func (oss *OSSClient) createBucket(bucketName string) error {
	_, err := oss.s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		return err
	}

	err = oss.s3Client.WaitUntilBucketExists(&s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		return err
	}

	return nil
}

func (oss *OSSClient) ListObjects(prefix string) ([]OSSObject, error) {
	bucketName := oss.dataBucket
	switch prefix {
	case "cache":
		bucketName = oss.proofBucket
	}

	objs, err := oss.s3Client.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return nil, err
	}

	log.Infof("%v", objs)

	return nil, nil
}
