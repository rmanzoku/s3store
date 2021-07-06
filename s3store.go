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
}

func NewS3Store(bucket string) (*S3Store, error) {
	s := new(S3Store)
	s.Bucket = bucket

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}
	s.Uploader = manager.NewUploader(s3.NewFromConfig(cfg), func(u *manager.Uploader) {
		u.BufferProvider = manager.NewBufferedReadSeekerWriteToPool(25 * 1024 * 1024)
	})

	s.Downloader = manager.NewDownloader(s3.NewFromConfig(cfg), func(d *manager.Downloader) {
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
