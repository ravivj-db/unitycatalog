package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CreateStagingTable;
import io.unitycatalog.server.model.StagingTableInfo;
import io.unitycatalog.server.persist.TableRepository;

@ExceptionHandler(GlobalExceptionHandler.class)
public class StagingTableService {

  private static final TableRepository TABLE_REPOSITORY = TableRepository.getInstance();

  @Post("")
  public HttpResponse createStagingTable(CreateStagingTable createStagingTable) {
    assert createStagingTable != null;
    StagingTableInfo createdStagingTable = TABLE_REPOSITORY.createStagingTable(createStagingTable);
    return HttpResponse.ofJson(createdStagingTable);
  }
}
