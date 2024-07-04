package io.unitycatalog.server.service;

import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CreateTable;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.CreateStagingTable;
import io.unitycatalog.server.model.StagingTableInfo;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.*;
import io.unitycatalog.server.persist.TableRepository;
import com.linecorp.armeria.common.HttpResponse;

import java.util.Optional;

@ExceptionHandler(GlobalExceptionHandler.class)
public class TableService {

    private static final TableRepository databaseOperations = TableRepository.getInstance();
    public TableService() {}

    @Post("/staging-tables")
    public HttpResponse createStagingTable(CreateStagingTable createStagingTable) {
        assert createStagingTable != null;
        StagingTableInfo createdStagingTable = databaseOperations.createStagingTable(createStagingTable);
        return HttpResponse.ofJson(createdStagingTable);
    }

    @Post("/tables")
    public HttpResponse createTable(CreateTable createTable) {
        assert createTable != null;
        TableInfo createTableResponse = databaseOperations.createTable(createTable);
        return HttpResponse.ofJson(createTableResponse);
    }


    @Get("/tables/{full_name}")
    public HttpResponse getTable(@Param("full_name") String fullName) {
        assert fullName != null;
        TableInfo tableInfo = databaseOperations.getTable(fullName);
        return HttpResponse.ofJson(tableInfo);
    }

    @Get("/tables")
    public HttpResponse listTables(
            @Param("catalog_name") String catalogName,
            @Param("schema_name") String schemaName,
            @Param("max_results") Optional<Integer> maxResults,
            @Param("page_token") Optional<String> pageToken,
            @Param("omit_properties") Optional<Boolean> omitProperties,
            @Param("omit_columns") Optional<Boolean> omitColumns
    ) {
        return HttpResponse.ofJson(databaseOperations.
                listTables(catalogName,
                        schemaName,
                        maxResults.orElse(100),
                        pageToken.orElse(null),
                        omitProperties.orElse(false),
                        omitColumns.orElse(false)));
    }

    @Delete("/tables/{full_name}")
    public HttpResponse deleteTable(@Param("full_name") String fullName) {
        databaseOperations.deleteTable(fullName);
        return HttpResponse.of(HttpStatus.OK);
    }


}
