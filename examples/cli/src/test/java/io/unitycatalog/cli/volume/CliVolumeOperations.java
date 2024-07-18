package io.unitycatalog.cli.volume;

import static io.unitycatalog.cli.TestUtils.addServerAndAuthParams;
import static io.unitycatalog.cli.TestUtils.executeCLICommand;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.model.CreateVolumeRequestContent;
import io.unitycatalog.client.model.UpdateVolumeRequestContent;
import io.unitycatalog.client.model.VolumeInfo;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.volume.VolumeOperations;
import java.util.ArrayList;
import java.util.List;

public class CliVolumeOperations implements VolumeOperations {

  private final ServerConfig config;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public CliVolumeOperations(ServerConfig config) {
    this.config = config;
  }

  @Override
  public VolumeInfo createVolume(CreateVolumeRequestContent createVolumeRequest) {
    List<String> argsList =
        new ArrayList<>(
            List.of(
                "volume",
                "create",
                "--full_name",
                createVolumeRequest.getCatalogName()
                    + "."
                    + createVolumeRequest.getSchemaName()
                    + "."
                    + createVolumeRequest.getName(),
                "--storage_location",
                createVolumeRequest.getStorageLocation()));
    if (createVolumeRequest.getComment() != null) {
      argsList.add("--comment");
      argsList.add(createVolumeRequest.getComment());
    }
    String[] args = addServerAndAuthParams(argsList, config);
    JsonNode volumeInfoJson = executeCLICommand(args);
    return objectMapper.convertValue(volumeInfoJson, VolumeInfo.class);
  }

  @Override
  public List<VolumeInfo> listVolumes(String catalogName, String schemaName) {
    String[] args =
        addServerAndAuthParams(
            List.of("volume", "list", "--catalog", catalogName, "--schema", schemaName), config);
    JsonNode volumeList = executeCLICommand(args);
    return objectMapper.convertValue(volumeList, new TypeReference<List<VolumeInfo>>() {});
  }

  @Override
  public VolumeInfo getVolume(String volumeFullName) {
    String[] args =
        addServerAndAuthParams(List.of("volume", "get", "--full_name", volumeFullName), config);
    JsonNode volumeInfoJson = executeCLICommand(args);
    return objectMapper.convertValue(volumeInfoJson, VolumeInfo.class);
  }

  @Override
  public VolumeInfo updateVolume(
      String volumeFullName, UpdateVolumeRequestContent updateVolumeRequest) {
    String[] args =
        addServerAndAuthParams(
            List.of(
                "volume",
                "update",
                "--full_name",
                volumeFullName,
                "--new_name",
                updateVolumeRequest.getNewName(),
                "--comment",
                updateVolumeRequest.getComment()),
            config);
    JsonNode updatedVolumeInfo = executeCLICommand(args);
    return objectMapper.convertValue(updatedVolumeInfo, VolumeInfo.class);
  }

  @Override
  public void deleteVolume(String volumeFullName) {
    String[] args =
        addServerAndAuthParams(List.of("volume", "delete", "--full_name", volumeFullName), config);
    executeCLICommand(args);
  }
}
