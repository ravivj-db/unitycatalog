package io.unitycatalog.server.persist.utils.hdfs.abfs;

import io.unitycatalog.server.model.AzureUserDelegationSAS;
import java.io.IOException;

import io.unitycatalog.server.persist.utils.hdfs.AbstractFileSystemHandler;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ABFSFileSystemHandler extends AbstractFileSystemHandler {

  private final Configuration conf;
  private static final Logger LOGGER = LoggerFactory.getLogger(ABFSFileSystemHandler.class);

  public ABFSFileSystemHandler(String storageRoot, AzureUserDelegationSAS azureCredential) {
    super(storageRoot);
    String containerAndAccount = this.storageRoot.getAuthority();

    // Split the authority part into container and account
    String[] parts = containerAndAccount.split("@");
    String containerName = parts[0];
    String accountName = parts[1].replace(".dfs.core.windows.net", "");

    conf = new Configuration();

    // filesystem implementation
    conf.set("fs.abfs.impl", "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem");
    conf.set("fs.abfss.impl", "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem");
    conf.set(
        "fs.defaultFS",
        "abfs://"
            + containerName
            + "@"
            + accountName
            + ".dfs.core.windows.net");

    // authorization
    conf.set(
        "fs.azure.account.auth.type." + accountName + ".dfs.core.windows.net", "SAS");
    conf.set(
        "fs.azure.sas.token.provider.type." + accountName + ".dfs.core.windows.net",
        ABFSSASTokenProvider.class.getName());
    conf.set(
        "fs.azure.sas.fixed.token." + accountName + ".dfs.core.windows.net",
        azureCredential.getSasToken());

    conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true");
  }

  @Override
  protected Configuration getHadoopConfiguration() {
    return conf;
  }

  @Override
  protected Logger getLogger() {
    return LOGGER;
  }

  public static void main(String[] args) throws IOException {
    String containerName = "default";
    String accountName = "adlstrial22";

    String storageRoot = "abfs://" + containerName + "@" + accountName + ".dfs.core.windows.net";
    AzureUserDelegationSAS azureCredential =
        new AzureUserDelegationSAS()
            .sasToken(
                "");

    ABFSFileSystemHandler abfsFileSystemHandler =
        new ABFSFileSystemHandler(storageRoot, azureCredential);

    // abfsFileSystemHandler.createDirectory("/a/b/c/d");
    // abfsFileSystemHandler.createDirectory("/a/b/c/e");
    // abfsFileSystemHandler.createDirectory("/a/f/g/h");

    abfsFileSystemHandler.deleteDirectory("/a", true);
  }
}
