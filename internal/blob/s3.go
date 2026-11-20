package blob

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3Config points at an S3-compatible endpoint (AWS S3, MinIO, ...).
type S3Config struct {
	Endpoint string // host:port, e.g. s3.us-east-1.amazonaws.com or 127.0.0.1:19000
	Bucket   string
	// Prefix namespaces every key, so many clusters or test runs can share
	// one bucket (bucket names are global on AWS).
	Prefix string
	// Static keys. When AccessKey is empty the standard AWS chain is used:
	// AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, then the shared credentials
	// file (AWS_PROFILE), then the EC2/ECS instance role.
	AccessKey string
	SecretKey string
	Region    string
	UseTLS    bool
	// Create the bucket if it does not exist.
	CreateBucket bool
}

// S3 is a Store backed by an S3 bucket.
type S3 struct {
	c      *minio.Client
	bucket string
	prefix string
}

func NewS3(ctx context.Context, cfg S3Config) (*S3, error) {
	creds := credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, "")
	if cfg.AccessKey == "" {
		creds = credentials.NewChainCredentials([]credentials.Provider{
			&credentials.EnvAWS{},
			&credentials.FileAWSCredentials{},
			&credentials.IAM{},
		})
	}
	c, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  creds,
		Secure: cfg.UseTLS,
		Region: cfg.Region,
	})
	if err != nil {
		return nil, err
	}
	s := &S3{c: c, bucket: cfg.Bucket, prefix: cfg.Prefix}
	if cfg.CreateBucket {
		ok, err := c.BucketExists(ctx, cfg.Bucket)
		if err != nil {
			return nil, fmt.Errorf("s3: bucket check: %w", err)
		}
		if !ok {
			if err := c.MakeBucket(ctx, cfg.Bucket, minio.MakeBucketOptions{Region: cfg.Region}); err != nil {
				// Another node may have created it concurrently.
				if ok2, _ := c.BucketExists(ctx, cfg.Bucket); !ok2 {
					return nil, fmt.Errorf("s3: make bucket: %w", err)
				}
			}
		}
	}
	return s, nil
}

func (s *S3) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.c.PutObject(ctx, s.bucket, s.prefix+key, bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{ContentType: "application/octet-stream", DisableMultipart: true})
	return err
}

func (s *S3) Get(ctx context.Context, key string) ([]byte, error) {
	obj, err := s.c.GetObject(ctx, s.bucket, s.prefix+key, minio.GetObjectOptions{})
	if err != nil {
		return nil, mapErr(err)
	}
	defer obj.Close()
	b, err := io.ReadAll(obj)
	if err != nil {
		return nil, mapErr(err)
	}
	return b, nil
}

func (s *S3) Exists(ctx context.Context, key string) (bool, error) {
	_, err := s.c.StatObject(ctx, s.bucket, s.prefix+key, minio.StatObjectOptions{})
	if err == nil {
		return true, nil
	}
	if mapErr(err) == ErrNotFound {
		return false, nil
	}
	return false, err
}

func (s *S3) List(ctx context.Context, prefix string, fn func(string) error) error {
	for obj := range s.c.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: s.prefix + prefix, Recursive: true}) {
		if obj.Err != nil {
			return obj.Err
		}
		if err := fn(strings.TrimPrefix(obj.Key, s.prefix)); err != nil {
			return err
		}
	}
	return nil
}

func (s *S3) Delete(ctx context.Context, key string) error {
	return s.c.RemoveObject(ctx, s.bucket, s.prefix+key, minio.RemoveObjectOptions{})
}

// RemoveAll deletes every object under the prefix, and then the bucket
// itself if there is no prefix (test and benchmark cleanup).
func (s *S3) RemoveAll(ctx context.Context) error {
	objs := make(chan minio.ObjectInfo)
	go func() {
		defer close(objs)
		for o := range s.c.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: s.prefix, Recursive: true}) {
			if o.Err == nil {
				objs <- o
			}
		}
	}()
	for e := range s.c.RemoveObjects(ctx, s.bucket, objs, minio.RemoveObjectsOptions{}) {
		if e.Err != nil {
			return e.Err
		}
	}
	if s.prefix != "" {
		return nil
	}
	return s.c.RemoveBucket(ctx, s.bucket)
}

func mapErr(err error) error {
	if r := minio.ToErrorResponse(err); r.Code == "NoSuchKey" || r.StatusCode == 404 {
		return ErrNotFound
	}
	return err
}
