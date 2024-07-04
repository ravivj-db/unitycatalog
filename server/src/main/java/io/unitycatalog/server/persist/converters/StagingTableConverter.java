package io.unitycatalog.server.persist.converters;

import io.unitycatalog.server.model.CreateStagingTable;
import io.unitycatalog.server.model.StagingTableInfo;
import io.unitycatalog.server.persist.dao.StagingTableDAO;

import java.util.Date;
import java.util.UUID;

public class StagingTableConverter {

    public static StagingTableInfo convertFromCreateRequest(CreateStagingTable createStagingTable) {
        if (createStagingTable == null) {
            return null;
        }
        return new StagingTableInfo()
                .catalogName(createStagingTable.getCatalogName())
                .schemaName(createStagingTable.getSchemaName())
                .stagingLocation(createStagingTable.getStagingLocation())
                .name(createStagingTable.getName())
                .stagingLocation(createStagingTable.getStagingLocation());
    }

    public static StagingTableInfo convertToDTO(StagingTableDAO dao) {
        if (dao == null) {
            return null;
        }
        StagingTableInfo dto = new StagingTableInfo()
                .stagingLocation(dao.getStagingLocation());
        if (dao.getId() != null) {
            dto.id(dao.getId().toString());
        }
        return dto;
    }

    public static StagingTableDAO convertToDAO(StagingTableInfo dto) {
        if (dto == null) {
            return null;
        }
        StagingTableDAO dao = StagingTableDAO.builder().stagingLocation(dto.getStagingLocation()).build();
        if (dto.getId() != null) {
            dao.setId(UUID.fromString(dto.getId()));
        }
        return dao;
    }

    public static void setDefaultFields(StagingTableDAO dao) {
        dao.setCreatedAt(new Date()); // Assuming current date for creation
        dao.setAccessedAt(new Date()); // Assuming current date for last access
        dao.setStageCommitted(false);
        dao.setStageCommittedAt(null);
        dao.setPurgeState((short)0);
        dao.setNumCleanupRetries((short)0);
        dao.setLastCleanupAt(null);
    }

}