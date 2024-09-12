package io.unitycatalog.server.persist.utils;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.StagingTableInfo;
import io.unitycatalog.server.utils.Constants;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileUtils.class);
  private static final ServerPropertiesUtils properties = ServerPropertiesUtils.getInstance();

  private FileUtils() {}

  private static String getStorageRoot() {
    return properties.getProperty("storageRoot");
  }

  private static String getDirectoryURI(String entityFullName) {
    String absoluteDirectoryPath = getStorageRoot() + "/" + entityFullName.replace(".", "/");
    String standardizedUri = toStandardizedURIString(absoluteDirectoryPath);
    validateURI(standardizedUri);
    return standardizedUri;
  }

  public static String createVolumeDirectory(String volumeName) {
    String absoluteUri = getDirectoryURI(volumeName);
    return createDirectory(absoluteUri).toString();
  }

  public static String createTableDirectory(StagingTableInfo stagingTableInfo) {
    return createTableDirectory(stagingTableInfo.getId());
  }

  public static String createTableDirectory(String tableId) {
    String fileDirectoryUri = getDirectoryURI("tables." + tableId);
    // TODO : actual table directory creation will be handler in the future
    // createDirectory(fileDirectoryUri);
    return fileDirectoryUri;
  }

  private static URI createDirectory(String uri) {
    URI parsedUri = createURI(uri);
    if (uri.startsWith("s3://")) {
      return modifyS3Directory(parsedUri, true);
    } else {
      return createLocalDirectory(Paths.get(parsedUri));
    }
  }

  private static URI createURI(String uri) {
    return Paths.get(uri).toUri();
  }

  private static URI createLocalDirectory(Path dirPath) {
    // Check if directory already exists
    if (Files.exists(dirPath)) {
      throw new BaseException(ErrorCode.ALREADY_EXISTS, "Directory already exists: " + dirPath);
    }
    // Create the directory
    try {
      Files.createDirectories(dirPath);
      LOGGER.debug("Directory created successfully: " + dirPath);
    } catch (Exception e) {
      throw new BaseException(ErrorCode.INTERNAL, "Failed to create directory: " + dirPath, e);
    }
    return dirPath.toUri();
  }

  public static void deleteDirectory(String path) {
    URI directoryUri = createURI(path);
    validateURI(directoryUri);
    if (directoryUri.getScheme() == null || directoryUri.getScheme().equals("file")) {
      try {
        deleteLocalDirectory(Paths.get(directoryUri));
      } catch (RuntimeException | IOException e) {
        throw new BaseException(ErrorCode.INTERNAL, "Failed to delete directory: " + path, e);
      }
    } else if (directoryUri.getScheme().equals("s3")) {
      modifyS3Directory(directoryUri, false);
    } else {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "Unsupported URI scheme: " + directoryUri.getScheme());
    }
  }

  private static void deleteLocalDirectory(Path dirPath) throws IOException {
    if (Files.exists(dirPath)) {
      try (Stream<Path> walk = Files.walk(dirPath, FileVisitOption.FOLLOW_LINKS)) {
        walk.sorted(Comparator.reverseOrder())
            .forEach(
                path -> {
                  try {
                    Files.delete(path);
                  } catch (IOException e) {
                    throw new RuntimeException("Failed to delete " + path, e);
                  }
                });
      }
    } else {
      throw new IOException("Directory does not exist: " + dirPath);
    }
  }

  private static URI modifyS3Directory(URI parsedUri, boolean createOrDelete) {
    String bucketName = parsedUri.getHost();
    String path = parsedUri.getPath().substring(1); // Remove leading '/'
    String accessKey = ServerPropertiesUtils.getInstance().getProperty("aws.s3.accessKey");
    String secretKey = ServerPropertiesUtils.getInstance().getProperty("aws.s3.secretKey");
    String sessionToken = ServerPropertiesUtils.getInstance().getProperty("aws.s3.sessionToken");
    String region = ServerPropertiesUtils.getInstance().getProperty("aws.region");

    BasicSessionCredentials sessionCredentials =
        new BasicSessionCredentials(accessKey, secretKey, sessionToken);
    AmazonS3 s3Client =
        AmazonS3ClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(sessionCredentials))
            .withRegion(region)
            .build();

    if (createOrDelete) {

      if (!path.endsWith("/")) {
        path += "/";
      }
      if (s3Client.doesObjectExist(bucketName, path)) {
        throw new BaseException(ErrorCode.ALREADY_EXISTS, "Directory already exists: " + path);
      }
      try {
        // Create empty content
        byte[] emptyContent = new byte[0];
        ByteArrayInputStream emptyContentStream = new ByteArrayInputStream(emptyContent);

        // Set metadata for the empty content
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);
        s3Client.putObject(new PutObjectRequest(bucketName, path, emptyContentStream, metadata));
        LOGGER.debug("Directory created successfully: " + path);
        return URI.create(String.format("s3://%s/%s", bucketName, path));
      } catch (Exception e) {
        throw new BaseException(ErrorCode.INTERNAL, "Failed to create directory: " + path, e);
      }
    } else {
      ObjectListing listing;
      ListObjectsRequest req = new ListObjectsRequest().withBucketName(bucketName).withPrefix(path);
      do {
        listing = s3Client.listObjects(req);
        listing
            .getObjectSummaries()
            .forEach(
                object -> {
                  s3Client.deleteObject(bucketName, object.getKey());
                });
        req.setMarker(listing.getNextMarker());
      } while (listing.isTruncated());
      return URI.create(String.format("s3://%s/%s", bucketName, path));
    }
  }

  private static String adjustFileUri(String fileUri) {
    URI uri = URI.create(fileUri);
    String path = uri.getPath(); // Path part
    // Always ensure "file:///" format for localhost file URIs
    return "file:///" + (path != null ? (path.startsWith("/") ? path.substring(1) : path) : "");
  }

  public static String toStandardizedURIString(String inputPath) {
    try {
      // Check if the path is already a URI with a valid scheme
      URI uri = new URI(inputPath);
      if (uri.getScheme() != null) {
        // If it's a file URI, standardize it
        if ("file".equalsIgnoreCase(uri.getScheme())) {
          return adjustFileUri(uri.toString());
        } else if (Constants.SUPPORTED_SCHEMES.contains(uri.getScheme())) {
          return uri.toString();
        } else
          throw new BaseException(
              ErrorCode.INVALID_ARGUMENT, "Unsupported URI scheme: " + uri.getScheme());
      }
    } catch (URISyntaxException e) {
      // Not a valid URI, treat it as a file path
    }

    // Handle local file paths (absolute or relative)
    URI uri = Paths.get(inputPath).toUri();
    return uri.toString();
  }

  private static void validateURI(String uri) {
    URI parsedUri = URI.create(uri);
    validateURI(parsedUri);
  }

  private static void validateURI(URI uri) {
    if (uri.getScheme() == null) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Invalid path: " + uri.getPath());
    }
    URI normalized = uri.normalize();
    if (!normalized.getPath().startsWith(uri.getPath())) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Normalization failed: " + uri.getPath());
    }
  }
}
