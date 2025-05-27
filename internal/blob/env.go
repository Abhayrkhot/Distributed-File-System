package blob

import "os"

// S3ConfigFromEnv reads DFS_S3_ENDPOINT, DFS_S3_BUCKET, DFS_S3_ACCESS_KEY,
// DFS_S3_SECRET_KEY, DFS_S3_REGION and DFS_S3_TLS=1.
func S3ConfigFromEnv() (S3Config, bool) {
	ep := os.Getenv("DFS_S3_ENDPOINT")
	if ep == "" {
		return S3Config{}, false
	}
	bucket := os.Getenv("DFS_S3_BUCKET")
	if bucket == "" {
		bucket = "dfs-chunks"
	}
	return S3Config{
		Endpoint:  ep,
		Bucket:    bucket,
		AccessKey: os.Getenv("DFS_S3_ACCESS_KEY"),
		SecretKey: os.Getenv("DFS_S3_SECRET_KEY"),
		Region:    os.Getenv("DFS_S3_REGION"),
		UseTLS:    os.Getenv("DFS_S3_TLS") == "1",
	}, true
}
