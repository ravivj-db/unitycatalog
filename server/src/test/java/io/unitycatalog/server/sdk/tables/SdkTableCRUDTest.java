package io.unitycatalog.server.sdk.tables;

import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.ListTablesResponse;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.BaseTableCRUDTest;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.utils.TestUtils;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SdkTableCRUDTest extends BaseTableCRUDTest {

    @BeforeClass
    public static void setUpClass() {
        // Any static setup specific to this test class
    }

    @AfterClass
    public static void tearDownClass() {
        // Any static teardown specific to this test class
    }

    @Override
    protected CatalogOperations createCatalogOperations(ServerConfig config) {
        return new SdkCatalogOperations(TestUtils.createApiClient(config));
    }

    @Override
    protected SchemaOperations createSchemaOperations(ServerConfig config) {
        return new SdkSchemaOperations(TestUtils.createApiClient(config));
    }

    @Override
    protected TableOperations createTableOperations(ServerConfig config) {
        localTablesApi = new TablesApi(TestUtils.createApiClient(config));
        return new SdkTableOperations(TestUtils.createApiClient(config));
    }

    /**
     * The SDK tests elide the `Response` object and directly return the model object.
     * However, clients can interact with the REST API directly, so its signature needs to be tested as well.
     * <p>
     * The tests below use a direct `TableAPI` client to inspect the response object.
     */
    private TablesApi localTablesApi;

    @Test
    public void testListTablesWithNoNextPageTokenShouldReturnNull() throws Exception {
        createCommonResources();
        TableInfo testingTable = createDefaultTestingTable();
        ListTablesResponse resp = localTablesApi.listTables(testingTable.getCatalogName(), testingTable.getSchemaName(), 100, null);
        assertThat(resp.getNextPageToken()).isNotNull();
        assertThat(resp.getTables()).hasSize(1)
                .first()
                .usingRecursiveComparison()
                .ignoringFields("columns", "storageLocation")
                .isEqualTo(testingTable);
    }
}
