package io.unitycatalog.server.base.table;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.persist.utils.FileUtils;
import io.unitycatalog.server.utils.TestUtils;
import org.junit.*;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public abstract class BaseTableCRUDTest extends BaseCRUDTest {
    protected SchemaOperations schemaOperations;
    protected TableOperations tableOperations;
    private String schemaId = null;

    ColumnInfo columnInfo1 = new ColumnInfo().name("as_int").typeText("INTEGER")
            .typeJson("{\"type\": \"integer\"}")
            .typeName(ColumnTypeName.INT).typePrecision(10).typeScale(0).position(0)
            .comment("Integer column").nullable(true);
    ColumnInfo columnInfo2 = new ColumnInfo().name("as_string").typeText("VARCHAR(255)")
            .typeJson("{\"type\": \"string\", \"length\": \"255\"}")
            .typeName(ColumnTypeName.STRING).position(1)
            .comment("String column").nullable(true);

    protected abstract SchemaOperations createSchemaOperations(ServerConfig serverConfig);
    protected abstract TableOperations createTableOperations(ServerConfig serverConfig);

    @Before
    @Override
    public void setUp() {
        super.setUp();
        schemaOperations = createSchemaOperations(serverConfig);
        tableOperations = createTableOperations(serverConfig);
    }

    protected void createCommonResources() throws ApiException {
        // Common setup operations such as creating a catalog and schema
        CreateCatalog createCatalog = new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT);
        catalogOperations.createCatalog(createCatalog);
        SchemaInfo schemaInfo = schemaOperations.createSchema(new CreateSchema().name(TestUtils.SCHEMA_NAME).catalogName(TestUtils.CATALOG_NAME));
        schemaId = schemaInfo.getSchemaId();
    }

    @Test
    public void testTableCRUD() throws IOException, ApiException {
        Assert.assertThrows(Exception.class, () -> tableOperations.getTable(TestUtils.TABLE_FULL_NAME));
        createCommonResources();

        // Create a table
        System.out.println("Testing create table..");
        TableInfo tableInfo = createDefaultTestingTable();
        assertEquals(TestUtils.TABLE_NAME, tableInfo.getName());
        Assert.assertEquals(TestUtils.CATALOG_NAME, tableInfo.getCatalogName());
        Assert.assertEquals(TestUtils.SCHEMA_NAME, tableInfo.getSchemaName());
        assertNotNull(tableInfo.getTableId());

        // Get table
        System.out.println("Testing get table..");
        TableInfo tableInfo2 = tableOperations.getTable(TestUtils.TABLE_FULL_NAME);
        assertEquals(tableInfo, tableInfo2);

        Collection<ColumnInfo> columnInfos2 = tableInfo2.getColumns();
        assertEquals(2, columnInfos2.size());
        assertEquals(1, columnInfos2.stream().filter(c -> c.getName().equals("as_int")).count());
        assertEquals(1, columnInfos2.stream().filter(c -> c.getName().equals("as_string")).count());

        // List tables
        System.out.println("Testing list tables..");
        Iterable<TableInfo> tableInfos = tableOperations.listTables(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME);
        assertTrue(TestUtils.contains(tableInfos, tableInfo2, table -> table.equals(tableInfo2)));

        // Delete table
        System.out.println("Testing delete table..");
        tableOperations.deleteTable(TestUtils.TABLE_FULL_NAME);
        assertThrows(Exception.class, () -> tableOperations.getTable(TestUtils.TABLE_FULL_NAME));

        CreateTable createTableRequest = new CreateTable()
                .name(TestUtils.TABLE_NAME)
                .catalogName(TestUtils.CATALOG_NAME)
                .schemaName(TestUtils.SCHEMA_NAME)
                .columns(List.of(columnInfo1, columnInfo2))
                .properties(TestUtils.PROPERTIES)
                .comment(TestUtils.COMMENT)
                .tableType(TableType.MANAGED)
                .dataSourceFormat(DataSourceFormat.DELTA);
        tableInfo = tableOperations.createTable(createTableRequest);

        System.out.println("Testing get managed table..");
        TableInfo managedTable = tableOperations.getTable(TestUtils.TABLE_FULL_NAME);
        assertEquals(TestUtils.TABLE_NAME, managedTable.getName());
        Assert.assertEquals(TestUtils.CATALOG_NAME, managedTable.getCatalogName());
        Assert.assertEquals(TestUtils.SCHEMA_NAME, managedTable.getSchemaName());
        Assert.assertEquals("file:///tmp/tables/" + managedTable.getTableId() + "/", managedTable.getStorageLocation());
        Assert.assertEquals(TableType.MANAGED, managedTable.getTableType());
        Assert.assertEquals(DataSourceFormat.DELTA, managedTable.getDataSourceFormat());
        assertNotNull(managedTable.getCreatedAt());
        assertNotNull(managedTable.getTableId());

        System.out.println("Testing list managed tables..");
        List<TableInfo> managedTables = tableOperations.listTables(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME);
        TableInfo managedListTable = managedTables.get(0);
        assertEquals(TestUtils.TABLE_NAME, managedListTable.getName());
        Assert.assertEquals(TestUtils.CATALOG_NAME, managedListTable.getCatalogName());
        Assert.assertEquals(TestUtils.SCHEMA_NAME, managedListTable.getSchemaName());
        Assert.assertEquals("file:///tmp/tables/" + managedListTable.getTableId() + "/", managedListTable.getStorageLocation());
        Assert.assertEquals(TableType.MANAGED, managedListTable.getTableType());
        Assert.assertEquals(DataSourceFormat.DELTA, managedListTable.getDataSourceFormat());
        assertNotNull(managedListTable.getCreatedAt());
        assertNotNull(managedListTable.getTableId());

        // Now update the parent schema name
        schemaOperations.updateSchema(TestUtils.SCHEMA_FULL_NAME, new UpdateSchema().newName(TestUtils.SCHEMA_NEW_NAME).comment(TestUtils.SCHEMA_COMMENT));
        // now fetch the table again
        TableInfo managedTableAfterSchemaUpdate = tableOperations.getTable(TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NEW_NAME + "." + TestUtils.TABLE_NAME);
        assertEquals(managedTable.getTableId(), managedTableAfterSchemaUpdate.getTableId());

        // test delete parent schema when table exists
        assertThrows(Exception.class, () -> schemaOperations.deleteSchema(TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NEW_NAME, Optional.of(false)));

        // test force delete parent schema when table exists
        String newTableFullName = TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NEW_NAME + "." + TestUtils.TABLE_NAME;
        schemaOperations.deleteSchema(TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NEW_NAME, Optional.of(true));
        assertThrows(Exception.class, () -> tableOperations.getTable(newTableFullName));
        assertThrows(Exception.class, () -> schemaOperations.getSchema(TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NEW_NAME));

    }

    protected TableInfo createDefaultTestingTable() throws IOException, ApiException {

        CreateTable createTableRequest = new CreateTable()
                .name(TestUtils.TABLE_NAME)
                .catalogName(TestUtils.CATALOG_NAME)
                .schemaName(TestUtils.SCHEMA_NAME)
                .columns(List.of(columnInfo1, columnInfo2))
                .properties(TestUtils.PROPERTIES)
                .comment(TestUtils.COMMENT)
                .storageLocation("/tmp/stagingLocation")
                .tableType(TableType.EXTERNAL)
                .dataSourceFormat(DataSourceFormat.DELTA);

        return tableOperations.createTable(createTableRequest);
    }
}