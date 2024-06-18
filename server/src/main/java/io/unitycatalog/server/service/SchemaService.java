package io.unitycatalog.server.service;

import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.CreateSchema;
import io.unitycatalog.server.model.SchemaInfo;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.*;
import io.unitycatalog.server.model.UpdateSchema;
import io.unitycatalog.server.persist.SchemaRepository;
import io.unitycatalog.server.utils.ValidationUtils;

import java.util.Optional;

import static io.unitycatalog.server.utils.ValidationUtils.CATALOG;
import static io.unitycatalog.server.utils.ValidationUtils.SCHEMA;

@ExceptionHandler(GlobalExceptionHandler.class)
public class SchemaService {
    private static final SchemaRepository schemaOperations = SchemaRepository.getInstance();

    public SchemaService() {}

    @Post("")
    public HttpResponse createSchema(CreateSchema createSchema) {
        return HttpResponse.ofJson(schemaOperations.createSchema(createSchema));
    }

    @Get("")
    public HttpResponse listSchemas(
            @Param("catalog_name") String catalogName,
            @Param("max_results") Optional<Integer> maxResults,
            @Param("page_token") Optional<String> pageToken) {
        return HttpResponse.ofJson(schemaOperations.listSchemas(catalogName, maxResults, pageToken));
    }

    @Get("/{full_name}")
    public HttpResponse getSchema(@Param("full_name") String fullName) {
        return HttpResponse.ofJson(schemaOperations.getSchema(fullName));
    }

    @Patch("/{full_name}")
    public HttpResponse updateSchema(@Param("full_name") String fullName, UpdateSchema updateSchema) {
        return HttpResponse.ofJson(schemaOperations.updateSchema(fullName, updateSchema));
    }

    @Delete("/{full_name}")
    public HttpResponse deleteSchema(@Param("full_name") String fullName, @Param("force") Optional<Boolean> force) {
        schemaOperations.deleteSchema(fullName, force.orElse(false));
        return HttpResponse.of(HttpStatus.OK);
    }
}
