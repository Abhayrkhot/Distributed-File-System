package storage

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	dfsv1 "github.com/Abhayrkhot/Distributed-File-System/gen/dfs/v1"
	"github.com/Abhayrkhot/Distributed-File-System/internal/coord"
	"github.com/Abhayrkhot/Distributed-File-System/internal/metastore"
	"github.com/Abhayrkhot/Distributed-File-System/internal/rpcutil"
)

// fileServer is the client-facing FileService; this node coordinates.
type fileServer struct {
	n *Node
	dfsv1.UnimplementedFileServiceServer
}

func fileInfo(f metastore.File, withChunks bool) *dfsv1.FileInfo {
	fi := &dfsv1.FileInfo{Name: f.Name, Version: f.Version, Size: f.Size, Sha256: f.SHA256, CreatedUnixMs: f.Created.UnixMilli()}
	if withChunks {
		for _, c := range f.Chunks {
			fi.Chunks = append(fi.Chunks, &dfsv1.ChunkRef{Id: c.ID, Size: c.Size})
		}
	}
	return fi
}

func fileErr(err error) error {
	switch {
	case errors.Is(err, metastore.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, coord.ErrQuorum), errors.Is(err, coord.ErrNoRing), errors.Is(err, coord.ErrChunkMissing):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Error(codes.Internal, err.Error())
}

func (s *fileServer) Upload(stream dfsv1.FileService_UploadServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	h := first.GetHeader()
	if h == nil || h.Name == "" {
		return status.Error(codes.InvalidArgument, "first message must be a header with a name")
	}
	r := &streamReader{recv: func() ([]byte, error) {
		m, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		return m.GetData(), nil
	}}
	f, err := s.n.coord.Upload(stream.Context(), h.Name, r)
	if err != nil {
		return fileErr(err)
	}
	return stream.SendAndClose(fileInfo(f, false))
}

type frameWriter struct {
	send func([]byte) error
}

func (w *frameWriter) Write(p []byte) (int, error) {
	for off := 0; off < len(p); off += rpcutil.FrameSize {
		end := min(off+rpcutil.FrameSize, len(p))
		if err := w.send(p[off:end]); err != nil {
			return off, err
		}
	}
	return len(p), nil
}

func (s *fileServer) Download(req *dfsv1.DownloadRequest, stream dfsv1.FileService_DownloadServer) error {
	f, err := s.n.cfg.Metastore.GetFile(stream.Context(), req.Name, req.Version)
	if err != nil {
		return fileErr(err)
	}
	if err := stream.Send(&dfsv1.DownloadResponse{Msg: &dfsv1.DownloadResponse_Info{Info: fileInfo(f, false)}}); err != nil {
		return err
	}
	w := &frameWriter{send: func(b []byte) error {
		return stream.Send(&dfsv1.DownloadResponse{Msg: &dfsv1.DownloadResponse_Data{Data: b}})
	}}
	return fileErr(s.n.coord.Download(stream.Context(), f, w))
}

func (s *fileServer) Stat(ctx context.Context, req *dfsv1.StatRequest) (*dfsv1.FileInfo, error) {
	f, err := s.n.cfg.Metastore.GetFile(ctx, req.Name, req.Version)
	if err != nil {
		return nil, fileErr(err)
	}
	return fileInfo(f, true), nil
}

func (s *fileServer) List(ctx context.Context, req *dfsv1.ListRequest) (*dfsv1.ListResponse, error) {
	fs, err := s.n.cfg.Metastore.ListFiles(ctx, req.Prefix, int(req.Limit))
	if err != nil {
		return nil, fileErr(err)
	}
	out := &dfsv1.ListResponse{}
	for _, f := range fs {
		out.Files = append(out.Files, fileInfo(f, false))
	}
	return out, nil
}

func (s *fileServer) Delete(ctx context.Context, req *dfsv1.DeleteRequest) (*dfsv1.DeleteResponse, error) {
	if err := s.n.cfg.Metastore.DeleteFile(ctx, req.Name); err != nil {
		return nil, fileErr(err)
	}
	return &dfsv1.DeleteResponse{}, nil
}
