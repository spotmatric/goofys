package goofys

import (
	"github.com/kahing/goofys/internal"

	"context"
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseutil"
	"github.com/jinzhu/copier"
	"github.com/sirupsen/logrus"
)

var log = GetLogger("main")

type Config struct {
	// File system
	MountOptions map[string]string
	MountPoint   string

	Cache    []string
	DirMode  os.FileMode
	FileMode os.FileMode
	Uid      uint32
	Gid      uint32

	// S3
	Endpoint       string
	Region         string
	RegionSet      bool
	StorageClass   string
	RequesterPays  bool
	AccessKey      string
	SecretKey      string
	Profile        string
	UseContentType bool
	UseSSE         bool
	UseKMS         bool
	KMSKeyID       string
	ACL            string
	Subdomain      bool

	// Tuning
	Cheap        bool
	ExplicitDir  bool
	StatCacheTTL time.Duration
	TypeCacheTTL time.Duration
	HTTPTimeout  time.Duration

	// Debugging
	DebugFuse  bool
	DebugS3    bool
	Foreground bool
}

func Mount(
	ctx context.Context,
	bucketName string,
	config *Config) (fs *Goofys, mfs *fuse.MountedFileSystem, err error) {

	var flags FlagStorage
	copier.Copy(&flags, config)

	awsConfig := internal.NewAwsConfig(&flags)
	if flags.DebugS3 {
		internal.SetCloudLogLevel(logrus.DebugLevel)
	}
	// Mount the file system.
	mountCfg := &fuse.MountConfig{
		FSName:                  bucketName,
		Options:                 flags.MountOptions,
		ErrorLogger:             GetStdLogger(NewLogger("fuse"), logrus.ErrorLevel),
		DisableWritebackCaching: true,
	}

	if flags.DebugFuse {
		fuseLog := GetLogger("fuse")
		fuseLog.Level = logrus.DebugLevel
		log.Level = logrus.DebugLevel
		mountCfg.DebugLogger = GetStdLogger(fuseLog, logrus.DebugLevel)
	}

	if config.AccessKey != "" {
		awsConfig.Credentials = credentials.NewStaticCredentials(config.AccessKey, config.SecretKey, "")
	} else if len(flags.Profile) > 0 {
		awsConfig.Credentials = credentials.NewSharedCredentials("", flags.Profile)
	} else {
		if spec, err := internal.ParseBucketSpec(bucketName); err == nil {
			switch spec.Scheme {
			case "adl":
				auth, err := internal.AzureAuthorizerConfig{}.Authorizer()
				if err != nil {
					err = fmt.Errorf("couldn't load azure credentials: %v",
						err)
					return nil, nil, err
				}
				flags.Backend = &internal.ADLv1Config{
					Endpoint:   spec.Bucket,
					Authorizer: auth,
				}
				// adlv1 doesn't really have bucket
				// names, but we will rebuild the
				// prefix
				bucketName = ""
				if spec.Prefix != "" {
					bucketName = ":" + spec.Prefix
				}
			case "wasb":
				config, err := internal.AzureBlobConfig(flags.Endpoint)
				if err != nil {
					return nil, nil, err
				}
				flags.Backend = &config
				bucketName = spec.Bucket
				if spec.Prefix != "" {
					bucketName = ":" + spec.Prefix
				}
			}
		}
	}

	if len(flags.Endpoint) > 0 {
		awsConfig.Endpoint = &flags.Endpoint
	}

	awsConfig.S3ForcePathStyle = aws.Bool(!flags.Subdomain)

	fs = NewGoofys(ctx, bucketName, awsConfig, &flags)
	if fs == nil {
		err = fmt.Errorf("Mount: initialization failed")
		return
	}
	server := fuseutil.NewFileSystemServer(fs)

	mfs, err = fuse.Mount(flags.MountPoint, server, mountCfg)
	if err != nil {
		err = fmt.Errorf("Mount: %v", err)
		return
	}

	if len(flags.Cache) != 0 {
		log.Infof("Starting catfs %v", flags.Cache)
		catfs := exec.Command("catfs", flags.Cache...)
		lvl := logrus.InfoLevel
		if flags.DebugFuse {
			lvl = logrus.DebugLevel
			catfs.Env = append(catfs.Env, "RUST_LOG=debug")
		} else {
			catfs.Env = append(catfs.Env, "RUST_LOG=info")
		}
		catfsLog := GetLogger("catfs")
		catfsLog.Formatter.(*LogHandle).Lvl = &lvl
		catfs.Stderr = catfsLog.Writer()
		err = catfs.Start()
		if err != nil {
			err = fmt.Errorf("Failed to start catfs: %v", err)

			// sleep a bit otherwise can't unmount right away
			time.Sleep(time.Second)
			err2 := TryUnmount(flags.MountPoint)
			if err2 != nil {
				err = fmt.Errorf("%v. Failed to unmount: %v", err, err2)
			}
		}

		go func() {
			err := catfs.Wait()
			log.Errorf("catfs exited: %v", err)

			if err != nil {
				// if catfs terminated cleanly, it
				// should have unmounted this,
				// otherwise we will do it ourselves
				err2 := TryUnmount(flags.MountPointArg)
				if err2 != nil {
					log.Errorf("Failed to unmount: %v", err2)
				}
			}

			if flags.MountPointArg != flags.MountPoint {
				err2 := TryUnmount(flags.MountPoint)
				if err2 != nil {
					log.Errorf("Failed to unmount: %v", err2)
				}
			}

			if err != nil {
				os.Exit(1)
			}
		}()
	}

	return
}

// expose Goofys related functions and types for extending and mounting elsewhere
var (
	GetStdLogger      = internal.GetStdLogger
	InitLoggers       = internal.InitLoggers
	MassageMountFlags = internal.MassageMountFlags
	GetLogger         = internal.GetLogger
	NewGoofys         = internal.NewGoofys
	NewLogger         = internal.NewLogger
	TryUnmount        = internal.TryUnmount
	MyUserAndGroup    = internal.MyUserAndGroup
)

type (
	Goofys      = internal.Goofys
	FlagStorage = internal.FlagStorage
	LogHandle   = internal.LogHandle
)
