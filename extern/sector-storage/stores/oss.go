package stores

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"golang.org/x/xerrors"
	"os"
	"strings"
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
	s3Client     *s3.S3
	s3Uploader   *s3manager.Uploader
	s3Downloader *s3manager.Downloader
	s3Session    *session.Session
	s3Info       OSSInfo
	proofBucket  string
	dataBucket   string
}

type OSSSector struct {
	name string
}

func (obj *OSSSector) Name() string {
	return obj.name
}

const ossKeySeparator = "/"

func (info *OSSInfo) ProofBucket() string {
	return fmt.Sprintf("%s-%s-proof", info.BucketName, info.Prefix)
}

func (info *OSSInfo) DataBucket() string {
	return fmt.Sprintf("%s-%s-data", info.BucketName, info.Prefix)
}

func (info *OSSInfo) Equal(another *OSSInfo) bool {
	return info.URL == another.URL &&
		info.AccessKey == another.AccessKey &&
		info.SecretKey == another.SecretKey &&
		info.BucketName == another.BucketName &&
		info.Prefix == another.Prefix
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

	log.Debugf("buckets from %v", info.URL)
	log.Debugf("%v", buckets)

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
		return nil, fmt.Errorf("bucket %v is not exists", ossCli.proofBucket)
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
		return nil, fmt.Errorf("bucket %v is not exists", ossCli.dataBucket)
	}

	ossCli.s3Uploader = s3manager.NewUploader(ossCli.s3Session)
	ossCli.s3Downloader = s3manager.NewDownloader(ossCli.s3Session)

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

func (oss *OSSClient) bucketNameByPrefix(prefix string) (string, error) {
	switch prefix {
	case "cache":
		return oss.proofBucket, nil
	case "sealed":
		return oss.dataBucket, nil
	case "unsealed":
		return oss.dataBucket, nil
	}
	return "", xerrors.Errorf("invalid prefix value %v", prefix)
}

func (oss *OSSClient) ListSectors(prefix string) ([]OSSSector, error) {
	bucketName, err := oss.bucketNameByPrefix(prefix)
	if err != nil {
		return nil, err
	}

	maxKeys := int64(10000000)
	objs, err := oss.s3Client.ListObjects((&s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefix),
	}).SetMaxKeys(maxKeys))
	if err != nil {
		return nil, err
	}

	ossObjs := []OSSSector{}
	sectorFind := map[string]struct{}{}

	for _, obj := range objs.Contents {
		keys := strings.Split(*obj.Key, ossKeySeparator)
		if len(keys) < 2 {
			return nil, xerrors.Errorf("error key %v from bucket %v", obj.Key, bucketName)
		}
		sectorName := keys[1]
		if _, ok := sectorFind[sectorName]; ok {
			continue
		}
		ossObjs = append(ossObjs, OSSSector{
			name: sectorName,
		})
		sectorFind[sectorName] = struct{}{}
	}

	return ossObjs, nil
}

func (oss *OSSClient) UploadObject(prefix string, objName string, path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	bucketName, err := oss.bucketNameByPrefix(prefix)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("%v%v%v", prefix, ossKeySeparator, objName)

	_, err = oss.s3Uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   file,
	})
	if err != nil {
		return err
	}

	err = oss.s3Client.WaitUntilObjectExists(&s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return err
	}

	return nil
}
