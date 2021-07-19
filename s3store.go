package s3store

import (
	"bytes"
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Store struct {
	Bucket     string
	Uploader   *manager.Uploader
	Downloader *manager.Downloader
	Client     *s3.Client
}

func NewS3Store(bucket string) (*S3Store, error) {
	s := new(S3Store)
	s.Bucket = bucket

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}

	s.Client = s3.NewFromConfig(cfg)

	s.Uploader = manager.NewUploader(s.Client, func(u *manager.Uploader) {
		u.BufferProvider = manager.NewBufferedReadSeekerWriteToPool(25 * 1024 * 1024)
	})

	s.Downloader = manager.NewDownloader(s.Client, func(d *manager.Downloader) {
		d.PartSize = 64 * 1024 * 1024 // 64MB per part
	})

	return s, nil
}

func (s *S3Store) Put(key string, body []byte) (err error) {
	return s.PutWithContentType(context.TODO(), key, body, "application/octet-stream")
}

func (s *S3Store) PutWithContentType(ctx context.Context, key string, body []byte, contentType string) (err error) {
	params := &s3.PutObjectInput{
		Bucket:      aws.String(s.Bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(body),
		ContentType: aws.String(contentType),
	}
	return s.PutRaw(ctx, params)
}

func (s *S3Store) PutWithMetadata(ctx context.Context, key string, body []byte, metadata map[string]string) (err error) {
	params := &s3.PutObjectInput{
		Bucket:   aws.String(s.Bucket),
		Key:      aws.String(key),
		Body:     bytes.NewReader(body),
		Metadata: metadata,
	}
	return s.PutRaw(ctx, params)
}

func (s *S3Store) PutRaw(ctx context.Context, params *s3.PutObjectInput) (err error) {
	_, err = s.Uploader.Upload(ctx, params)
	return
}

func (s *S3Store) Get(key string) ([]byte, error) {
	return s.GetWithCtx(context.TODO(), key)
}

func (s *S3Store) GetWithCtx(ctx context.Context, key string) ([]byte, error) {
	buffer := manager.NewWriteAtBuffer([]byte{})

	_, err := s.Downloader.Download(ctx, buffer, &s3.GetObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func (s *S3Store) List(prefix string) ([]string, error) {
	return s.ListWithCtx(context.TODO(), prefix)
}

func (s *S3Store) ListWithCtx(ctx context.Context, prefix string) ([]string, error) {
	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.Bucket),
		Prefix: aws.String(prefix),
	}
	output, err := s.Client.ListObjectsV2(ctx, params)
	if err != nil {
		return nil, err
	}

	keys := make([]string, output.KeyCount)
	for i, v := range output.Contents {
		keys[i] = *v.Key
	}

	return keys, nil
}
