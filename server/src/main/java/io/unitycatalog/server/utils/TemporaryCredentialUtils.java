package io.unitycatalog.server.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.AwsCredentials;
import io.unitycatalog.server.persist.utils.ServerPropertiesUtils;

public class TemporaryCredentialUtils {
  public static AwsCredentials findS3BucketConfig(String storageLocation) {
    ServerPropertiesUtils.S3BucketConfig s3BucketConfig =
        ServerPropertiesUtils.getInstance().getS3BucketConfig(storageLocation);
    if (s3BucketConfig == null) {
      throw new BaseException(ErrorCode.FAILED_PRECONDITION, "S3 bucket configuration not found.");
    }
    return new AwsCredentials()
        .accessKeyId(s3BucketConfig.getAccessKey())
        .secretAccessKey(s3BucketConfig.getSecretKey())
        .sessionToken(s3BucketConfig.getSessionToken());
  }
}
