package io.unitycatalog.server.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.persist.utils.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class FileUtilsTest {

    @Test
    public void testFileUtils() {

        System.setProperty("storageRoot", "/tmp");

        String tableId = UUID.randomUUID().toString();

        String tablePath = FileUtils.createTableDirectory(tableId);

        assertThat(tablePath).isEqualTo("file:///tmp/tables/" + tableId + "/");

        FileUtils.deleteDirectory(tablePath);

        System.setProperty("storageRoot", "file:///tmp/random");

        tablePath = FileUtils.createTableDirectory(tableId);

        assertThat(tablePath).isEqualTo("file:///tmp/random/tables/" + tableId + "/");

        FileUtils.deleteDirectory(tablePath);

        Assert.assertThrows(BaseException.class, () -> {
            FileUtils.createTableDirectory("..");
        });


    }
}
