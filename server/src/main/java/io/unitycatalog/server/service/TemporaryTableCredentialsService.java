package io.unitycatalog.server.service;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.*;
import io.unitycatalog.server.persist.PropertiesUtil;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.utils.TemporaryCredentialUtils;

@ExceptionHandler(GlobalExceptionHandler.class)
public class TemporaryTableCredentialsService {

    private static final TableRepository tableRepository = TableRepository.getInstance();

    @Post("")
    public HttpResponse generateTemporaryTableCredential(GenerateTemporaryTableCredential generateTemporaryTableCredential) {
        String tableId = generateTemporaryTableCredential.getTableId();

        // Check if table exists
        String tableStorageLocation = "";
        try {
            StagingTableInfo tableInfo = tableRepository.getStagingTableById(tableId);
            tableStorageLocation = tableInfo.getStagingLocation();
        } catch (RuntimeException ignored) {
            // try finding table in main table repository
        }
        if (tableStorageLocation.isEmpty()) {
            TableInfo tableInfo = tableRepository.getTableById(tableId);
            tableStorageLocation = tableInfo.getStorageLocation();
        }

        // Generate temporary credentials
        if (tableStorageLocation == null || tableStorageLocation.isEmpty()) {
            throw new BaseException(ErrorCode.FAILED_PRECONDITION, "Table storage location not found.");
        }
        if (tableStorageLocation.startsWith("s3://")) {
            return HttpResponse.ofJson(new GenerateTemporaryTableCredentialResponse()
               .awsTempCredentials(TemporaryCredentialUtils.findS3BucketConfig(tableStorageLocation)));

        } else {
            // return empty credentials for local file system
            return HttpResponse.ofJson(new GenerateTemporaryTableCredentialResponse());
        }
    }

}
