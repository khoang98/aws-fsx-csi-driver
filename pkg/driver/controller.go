/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package driver

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"sigs.k8s.io/aws-fsx-csi-driver/pkg/cloud"
	"sigs.k8s.io/aws-fsx-csi-driver/pkg/driver/internal"
	"sigs.k8s.io/aws-fsx-csi-driver/pkg/util"
)

var (
	// volumeCaps represents how the volume could be accessed.
	volumeCaps = []csi.VolumeCapability_AccessMode{
		{
			Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		},
		{
			Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
		},
	}

	// controllerCaps represents the capability of controller service
	controllerCaps = []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
	}
)

const (
	volumeContextDnsName                      = "dnsname"
	volumeContextMountName                    = "mountname"
	volumeParamsSubnetId                      = "subnetId"
	volumeParamsSecurityGroupIds              = "securityGroupIds"
	volumeParamsAutoImportPolicy              = "autoImportPolicy"
	volumeParamsS3ImportPath                  = "s3ImportPath"
	volumeParamsS3ExportPath                  = "s3ExportPath"
	volumeParamsDeploymentType                = "deploymentType"
	volumeParamsKmsKeyId                      = "kmsKeyId"
	volumeParamsPerUnitStorageThroughput      = "perUnitStorageThroughput"
	volumeParamsStorageType                   = "storageType"
	volumeParamsDriveCacheType                = "driveCacheType"
	volumeParamsAutomaticBackupRetentionDays  = "automaticBackupRetentionDays"
	volumeParamsDailyAutomaticBackupStartTime = "dailyAutomaticBackupStartTime"
	volumeParamsCopyTagsToBackups             = "copyTagsToBackups"
	volumeParamsDataCompressionType           = "dataCompressionType"
	volumeParamsWeeklyMaintenanceStartTime    = "weeklyMaintenanceStartTime"
	volumeParamsFileSystemTypeVersion         = "fileSystemTypeVersion"
	volumeParamsExtraTags                     = "extraTags"
	volumeParamsEfaEnabled                    = "efaEnabled"
	volumeParamsMetadataConfigurationMode     = "metadataConfigurationMode"
	volumeParamsMetadataIops                  = "metadataIops"
)

const (
	// TokenValidityWindow is the maximum PVC age for which the ClientRequestToken
	// is considered valid. Set below the actual token expiry (~8h) to provide buffer.
	TokenValidityWindow = 7 * time.Hour
)

// controllerService represents the controller service of CSI driver
type controllerService struct {
	cloud         cloud.Cloud
	inFlight      *internal.InFlight
	driverOptions *DriverOptions
	kubeClient    kubernetes.Interface
	// fsCache maps volume name → filesystem ID for in-flight creations.
	// This is an in-memory optimization to avoid re-calling CreateFileSystem
	// on retries when the pod is still alive.
	fsCache   map[string]string
	fsCacheMu sync.RWMutex
	csi.UnimplementedControllerServer
}

// newControllerService creates a new controller service
// it panics if failed to create the service
func newControllerService(driverOptions *DriverOptions) controllerService {
	region := os.Getenv("AWS_REGION")
	if region == "" {
		klog.V(5).InfoS("[Debug] Retrieving region from metadata service")
		metadata, err := cloud.NewMetadataService(cloud.DefaultEC2MetadataClient, cloud.DefaultKubernetesAPIClient, region)
		if err != nil {
			klog.ErrorS(err, "Could not determine region from any metadata service. The region can be manually supplied via the AWS_REGION environment variable.")
			panic(err)
		}
		region = metadata.GetRegion()
	}

	klog.InfoS("regionFromSession Controller service", "region", region)

	cloudSrv, err := cloud.NewCloud(region)
	if err != nil {
		panic(err)
	}

	kubeClient, err := cloud.DefaultKubernetesAPIClient()
	if err != nil {
		klog.ErrorS(err, "Failed to create Kubernetes client")
		panic(err)
	}

	return controllerService{
		cloud:         cloudSrv,
		inFlight:      internal.NewInFlight(),
		driverOptions: driverOptions,
		kubeClient:    kubeClient,
		fsCache:       make(map[string]string),
	}
}
func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

func (d *controllerService) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	klog.V(4).InfoS("CreateVolume: called", "args", util.SanitizeRequest(req))
	volName := req.GetName()
	if len(volName) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume name not provided")
	}

	volCaps := req.GetVolumeCapabilities()
	if len(volCaps) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume capabilities not provided")
	}

	if !isValidVolumeCapabilities(volCaps) {
		return nil, status.Error(codes.InvalidArgument, "Volume capabilities not supported")
	}

	// check if a request is already in-flight
	if ok := d.inFlight.Insert(volName); !ok {
		msg := fmt.Sprintf("Create volume request for %s is already in progress", volName)
		return nil, status.Error(codes.Aborted, msg)
	}
	defer d.inFlight.Delete(volName)

	// If we already have the filesystem ID cached (same pod, previous attempt),
	// skip creation and go straight to waiting for it to become available.
	d.fsCacheMu.RLock()
	cachedFsId, hasCached := d.fsCache[volName]
	d.fsCacheMu.RUnlock()

	if hasCached {
		klog.V(2).InfoS("CreateVolume: found cached filesystem ID, skipping creation",
			"volumeName", volName, "fsId", cachedFsId)
		fs, err := d.cloud.DescribeFileSystem(ctx, cachedFsId)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "Failed to describe cached filesystem %s: %v", cachedFsId, err)
		}

		err = d.cloud.WaitForFileSystemAvailable(ctx, fs.FileSystemId)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "Filesystem is not ready: %v", err)
		}

		return newCreateVolumeResponse(fs), nil
	}

	// Verify PVC age to ensure ClientRequestToken is still valid.
	volumeParams := req.GetParameters()
	pvcName := volumeParams["csi.storage.k8s.io/pvc/name"]
	pvcNamespace := volumeParams["csi.storage.k8s.io/pvc/namespace"]
	if pvcName != "" && pvcNamespace != "" {
		pvc, err := d.kubeClient.CoreV1().PersistentVolumeClaims(pvcNamespace).Get(ctx, pvcName, metav1.GetOptions{})
		if err != nil {
			return nil, status.Errorf(codes.Internal, "Failed to get PVC %s/%s: %v", pvcNamespace, pvcName, err)
		}
		if time.Since(pvc.CreationTimestamp.Time) > TokenValidityWindow {
			return nil, status.Errorf(codes.DeadlineExceeded,
				"Provisioning timed out: PVC %s/%s age exceeds token validity window", pvcNamespace, pvcName)
		}
	}

	// Build filesystem options from parameters
	subnetId := volumeParams[volumeParamsSubnetId]
	securityGroupIds := volumeParams[volumeParamsSecurityGroupIds]
	fsOptions := &cloud.FileSystemOptions{
		SubnetId:         subnetId,
		SecurityGroupIds: strings.Split(securityGroupIds, ","),
	}

	if val, ok := volumeParams[volumeParamsAutoImportPolicy]; ok {
		fsOptions.AutoImportPolicy = val
	}

	if val, ok := volumeParams[volumeParamsS3ImportPath]; ok {
		fsOptions.S3ImportPath = val
	}

	if val, ok := volumeParams[volumeParamsS3ExportPath]; ok {
		fsOptions.S3ExportPath = val
	}

	if val, ok := volumeParams[volumeParamsDeploymentType]; ok {
		fsOptions.DeploymentType = val
	}

	if val, ok := volumeParams[volumeParamsKmsKeyId]; ok {
		fsOptions.KmsKeyId = val
	}

	if val, ok := volumeParams[volumeParamsDailyAutomaticBackupStartTime]; ok {
		fsOptions.DailyAutomaticBackupStartTime = val
	}

	if val, ok := volumeParams[volumeParamsAutomaticBackupRetentionDays]; ok {
		n, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "automaticBackupRetentionDays must be a number")
		}
		fsOptions.AutomaticBackupRetentionDays = int32(n)
	}

	if val, ok := volumeParams[volumeParamsCopyTagsToBackups]; ok {
		b, err := strconv.ParseBool(val)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "copyTagsToBackups must be a bool")
		}
		fsOptions.CopyTagsToBackups = b
	}

	if val, ok := volumeParams[volumeParamsStorageType]; ok {
		fsOptions.StorageType = val
	}

	if val, ok := volumeParams[volumeParamsDriveCacheType]; ok {
		fsOptions.DriveCacheType = val
	}

	if val, ok := volumeParams[volumeParamsDataCompressionType]; ok {
		fsOptions.DataCompressionType = val
	}

	if val, ok := volumeParams[volumeParamsPerUnitStorageThroughput]; ok {
		n, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "perUnitStorageThroughput must be a number")
		}
		fsOptions.PerUnitStorageThroughput = int32(n)
	}

	if val, ok := volumeParams[volumeParamsWeeklyMaintenanceStartTime]; ok {
		fsOptions.WeeklyMaintenanceStartTime = val
	}

	if val, ok := volumeParams[volumeParamsFileSystemTypeVersion]; ok {
		fsOptions.FileSystemTypeVersion = val
	}

	if val, ok := volumeParams[volumeParamsEfaEnabled]; ok {
		b, err := strconv.ParseBool(val)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "efaEnabled must be a bool")
		}
		fsOptions.EfaEnabled = b
	}

	if val, ok := volumeParams[volumeParamsMetadataConfigurationMode]; ok {
		fsOptions.MetadataConfigurationMode = val
	}

	if val, ok := volumeParams[volumeParamsMetadataIops]; ok {
		n, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "metadataIops must be a number")
		}
		fsOptions.MetadataIops = int32(n)
	}

	capRange := req.GetCapacityRange()
	if capRange == nil {
		fsOptions.CapacityGiB = cloud.DefaultVolumeSize
	} else {
		newSizeInt64 := util.RoundUpVolumeSize(capRange.GetRequiredBytes(), fsOptions.DeploymentType, fsOptions.StorageType, fsOptions.PerUnitStorageThroughput)
		newSizeGiB, err := util.ConvertToInt32(newSizeInt64)
		if err != nil {
			return nil, status.Errorf(codes.OutOfRange, "Request storage capacity %d GiB is too large for integer type", newSizeInt64)
		}
		fsOptions.CapacityGiB = newSizeGiB
	}

	var tagArray []string
	optionsTags := d.driverOptions.extraTags

	if optionsTags != "" {
		tagArray = strings.Split(optionsTags, ",")
	}

	if val, ok := volumeParams[volumeParamsExtraTags]; ok && len(val) > 0 {
		extraTags := strings.Split(val, ",")
		err := validateExtraTags(extraTags)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		tagArray = append(tagArray, extraTags...)
	}
	fsOptions.ExtraTags = tagArray

	// Call CreateFileSystem — the ClientRequestToken (volume name) provides
	// idempotency within the token validity window. On retry (after pod crash),
	// AWS returns the existing filesystem instead of creating a duplicate.
	fs, err := d.cloud.CreateFileSystem(ctx, volName, fsOptions)
	if err != nil {
		switch err {
		case cloud.ErrFsExistsDiffSize:
			return nil, status.Error(codes.AlreadyExists, err.Error())
		default:
			return nil, status.Errorf(codes.Internal, "Could not create volume %q: %v", volName, err)
		}
	}

	// Cache the filesystem ID so subsequent retries (same pod) skip creation entirely
	d.fsCacheMu.Lock()
	d.fsCache[volName] = fs.FileSystemId
	d.fsCacheMu.Unlock()

	err = d.cloud.WaitForFileSystemAvailable(ctx, fs.FileSystemId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Filesystem is not ready: %v", err)
	}

	// Clean up cache entry on success — provisioning is complete
	d.fsCacheMu.Lock()
	delete(d.fsCache, volName)
	d.fsCacheMu.Unlock()

	return newCreateVolumeResponse(fs), nil
}

func (d *controllerService) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	klog.V(4).InfoS("DeleteVolume: called", "args", util.SanitizeRequest(req))
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	// check if a request is already in-flight
	if ok := d.inFlight.Insert(volumeID); !ok {
		msg := fmt.Sprintf(internal.VolumeOperationAlreadyExistsErrorMsg, volumeID)
		return nil, status.Error(codes.Aborted, msg)
	}
	defer d.inFlight.Delete(volumeID)

	if err := d.cloud.DeleteFileSystem(ctx, volumeID); err != nil {
		if err == cloud.ErrNotFound {
			klog.V(4).InfoS("DeleteVolume: volume not found, returning with success")
			return &csi.DeleteVolumeResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "Could not delete volume ID %q: %v", volumeID, err)
	}
	return &csi.DeleteVolumeResponse{}, nil
}

func (d *controllerService) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (d *controllerService) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (d *controllerService) ControllerModifyVolume(ctx context.Context, req *csi.ControllerModifyVolumeRequest) (*csi.ControllerModifyVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (d *controllerService) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	klog.V(4).InfoS("ControllerGetCapabilities: called", "args", util.SanitizeRequest(req))
	var caps []*csi.ControllerServiceCapability
	for _, cap := range controllerCaps {
		c := &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: cap,
				},
			},
		}
		caps = append(caps, c)
	}
	return &csi.ControllerGetCapabilitiesResponse{Capabilities: caps}, nil
}

func (d *controllerService) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	klog.V(4).InfoS("GetCapacity: called", "args", util.SanitizeRequest(req))
	return nil, status.Error(codes.Unimplemented, "")
}

func (d *controllerService) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	klog.V(4).InfoS("ListVolumes: called", "args", util.SanitizeRequest(req))
	return nil, status.Error(codes.Unimplemented, "")
}

func (d *controllerService) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	klog.V(4).InfoS("ValidateVolumeCapabilities: called", "args", util.SanitizeRequest(req))
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	volCaps := req.GetVolumeCapabilities()
	if len(volCaps) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume capabilities not provided")
	}

	if _, err := d.cloud.DescribeFileSystem(ctx, volumeID); err != nil {
		if err == cloud.ErrNotFound {
			return nil, status.Error(codes.NotFound, "Volume not found")
		}
		return nil, status.Errorf(codes.Internal, "Could not get volume with ID %q: %v", volumeID, err)
	}

	confirmed := isValidVolumeCapabilities(volCaps)
	if confirmed {
		return &csi.ValidateVolumeCapabilitiesResponse{
			Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
				// TODO if volume context is provided, should validate it too
				// VolumeContext:      req.GetVolumeContext(),
				VolumeCapabilities: volCaps,
				// TODO if parameters are provided, should validate them too
				// Parameters:      req.GetParameters(),
			},
		}, nil
	} else {
		return &csi.ValidateVolumeCapabilitiesResponse{}, nil
	}
}

func isValidVolumeCapabilities(volCaps []*csi.VolumeCapability) bool {
	hasSupport := func(cap *csi.VolumeCapability) bool {
		for i := range volumeCaps {
			c := &volumeCaps[i]
			if c.GetMode() == cap.AccessMode.GetMode() {
				return true
			}
		}
		return false
	}

	foundAll := true
	for _, c := range volCaps {
		if !hasSupport(c) {
			foundAll = false
		}
	}
	return foundAll
}

func (d *controllerService) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (d *controllerService) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (d *controllerService) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (d *controllerService) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	klog.V(4).InfoS("ControllerExpandVolume: called", "args", util.SanitizeRequest(req))
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	capRange := req.GetCapacityRange()
	if capRange == nil {
		return nil, status.Error(codes.InvalidArgument, "Capacity range not provided")
	}

	fs, err := d.cloud.DescribeFileSystem(ctx, volumeID)
	if err != nil {
		if err == cloud.ErrNotFound {
			return nil, status.Errorf(codes.NotFound, "Filesystem not found: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "DescribeFileSystem failed: %v", err)
	}

	newSizeInt64 := util.RoundUpVolumeSize(capRange.GetRequiredBytes(), fs.DeploymentType, fs.StorageType, fs.PerUnitStorageThroughput)
	newSizeGiB, err := util.ConvertToInt32(newSizeInt64)
	if err != nil {
		return nil, status.Errorf(codes.OutOfRange, "Requested storage capacity %d GiB is too large for integer type", newSizeInt64)
	}
	if util.GiBToBytes(newSizeGiB) != capRange.GetRequiredBytes() {
		klog.V(4).Infof("ControllerExpandVolume: requested storage capacity of %d bytes has been rounded to a valid storage capacity of %d GiB", capRange.GetRequiredBytes(), newSizeGiB)
	}
	if capRange.GetLimitBytes() > 0 && util.GiBToBytes(newSizeGiB) > capRange.GetLimitBytes() {
		return nil, status.Errorf(codes.OutOfRange, "Requested storage capacity of %d bytes exceeds capacity limit of %d bytes.", util.GiBToBytes(newSizeGiB), capRange.GetLimitBytes())
	}
	if newSizeGiB <= fs.CapacityGiB {
		// Current capacity is sufficient to satisfy the request
		klog.V(4).InfoS("ControllerExpandVolume: current filesystem capacity matches or exceeds requested storage capacity, returning with success", "current capacity", fs.CapacityGiB, "requested capacity", newSizeGiB)
		return &csi.ControllerExpandVolumeResponse{
			CapacityBytes:         util.GiBToBytes(fs.CapacityGiB),
			NodeExpansionRequired: false,
		}, nil
	}

	finalGiB, err := d.cloud.ResizeFileSystem(ctx, volumeID, newSizeGiB)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "resize failed: %v", err)
	}

	err = d.cloud.WaitForFileSystemResize(ctx, fs.FileSystemId, finalGiB)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "filesystem is not resized: %v", err)
	}

	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         util.GiBToBytes(finalGiB),
		NodeExpansionRequired: false,
	}, nil
}

func (d *controllerService) ControllerGetVolume(ctx context.Context, req *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func newCreateVolumeResponse(fs *cloud.FileSystem) *csi.CreateVolumeResponse {
	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      fs.FileSystemId,
			CapacityBytes: util.GiBToBytes(fs.CapacityGiB),
			VolumeContext: map[string]string{
				volumeContextDnsName:   fs.DnsName,
				volumeContextMountName: fs.MountName,
			},
		},
	}
}

func validateExtraTags(tags []string) error {
	for _, tag := range tags {
		tagSplit := strings.Split(tag, "=")
		if len(tagSplit) != 2 {
			return fmt.Errorf("invalid extraTag %s was provided", tag)
		}
	}
	return nil
}
