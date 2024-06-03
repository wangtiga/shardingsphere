/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.infra.binder.segment.table;

import org.apache.shardingsphere.infra.binder.segment.select.projection.impl.ColumnProjection;
import org.apache.shardingsphere.infra.database.type.DatabaseTypeEngine;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereColumn;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereSchema;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereTable;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.column.ColumnSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.generic.AliasSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.generic.OwnerSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.generic.table.SimpleTableSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.generic.table.TableNameSegment;
import org.apache.shardingsphere.sql.parser.sql.common.value.identifier.IdentifierValue;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class TablesContextTest {
    
    @Test
    public void assertGetTableNames() {
        TablesContext tablesContext = new TablesContext(Arrays.asList(createTableSegment("table_1", "tbl_1"),
                createTableSegment("table_2", "tbl_2")), DatabaseTypeEngine.getDatabaseType("MySQL"));
        assertThat(tablesContext.getTableNames(), is(new HashSet<>(Arrays.asList("table_1", "table_2"))));
    }
    
    @Test
    public void assertInstanceCreatedWhenNoExceptionThrown() {
        SimpleTableSegment tableSegment = new SimpleTableSegment(new TableNameSegment(0, 10, new IdentifierValue("tbl")));
        tableSegment.setOwner(new OwnerSegment(0, 0, new IdentifierValue("schema")));
        new TablesContext(Collections.singleton(tableSegment), DatabaseTypeEngine.getDatabaseType("MySQL"));
        // TODO add assertion
    }
    
    @Test
    public void assertFindTableNameWhenSingleTable() {
        SimpleTableSegment tableSegment = createTableSegment("table_1", "tbl_1");
        ColumnSegment columnSegment = createColumnSegment(null, "col");
        Map<String, String> actual = new TablesContext(Collections.singletonList(tableSegment), DatabaseTypeEngine.getDatabaseType("MySQL"))
                .findTableNamesByColumnSegment(Collections.singletonList(columnSegment), mock(ShardingSphereSchema.class));
        assertFalse(actual.isEmpty());
        assertThat(actual.get("col"), is("table_1"));
    }
    
    @Test
    public void assertFindTableNameWhenColumnSegmentOwnerPresent() {
        SimpleTableSegment tableSegment1 = createTableSegment("table_1", "tbl_1");
        SimpleTableSegment tableSegment2 = createTableSegment("table_2", "tbl_2");
        ColumnSegment columnSegment = createColumnSegment("table_1", "col");
        Map<String, String> actual = new TablesContext(Arrays.asList(tableSegment1, tableSegment2), DatabaseTypeEngine.getDatabaseType("MySQL"))
                .findTableNamesByColumnSegment(Collections.singletonList(columnSegment), mock(ShardingSphereSchema.class));
        assertFalse(actual.isEmpty());
        assertThat(actual.get("table_1.col"), is("table_1"));
    }
    
    @Test
    public void assertFindTableNameWhenColumnSegmentOwnerAbsent() {
        SimpleTableSegment tableSegment1 = createTableSegment("table_1", "tbl_1");
        SimpleTableSegment tableSegment2 = createTableSegment("table_2", "tbl_2");
        ColumnSegment columnSegment = createColumnSegment(null, "col");
        Map<String, String> actual = new TablesContext(Arrays.asList(tableSegment1, tableSegment2), DatabaseTypeEngine.getDatabaseType("MySQL"))
                .findTableNamesByColumnSegment(Collections.singletonList(columnSegment), mock(ShardingSphereSchema.class));
        assertTrue(actual.isEmpty());
    }
    
    @Test
    public void assertFindTableNameWhenColumnSegmentOwnerAbsentAndSchemaMetaDataContainsColumn() {
        SimpleTableSegment tableSegment1 = createTableSegment("table_1", "tbl_1");
        SimpleTableSegment tableSegment2 = createTableSegment("table_2", "tbl_2");
        ShardingSphereSchema schema = mock(ShardingSphereSchema.class);
        when(schema.getAllColumnNames("table_1")).thenReturn(Collections.singletonList("col"));
        ColumnSegment columnSegment = createColumnSegment(null, "col");
        Map<String, String> actual = new TablesContext(Arrays.asList(tableSegment1, tableSegment2),
                DatabaseTypeEngine.getDatabaseType("MySQL")).findTableNamesByColumnSegment(Collections.singletonList(columnSegment), schema);
        assertFalse(actual.isEmpty());
        assertThat(actual.get("col"), is("table_1"));
    }
    
    @Test
    public void assertFindTableNameWhenColumnSegmentOwnerAbsentAndSchemaMetaDataContainsColumnInUpperCase() {
        SimpleTableSegment tableSegment1 = createTableSegment("TABLE_1", "TBL_1");
        SimpleTableSegment tableSegment2 = createTableSegment("TABLE_2", "TBL_2");
        ShardingSphereTable table = new ShardingSphereTable("TABLE_1",
                Collections.singletonList(new ShardingSphereColumn("COL", 0, false, false, true, true, false)), Collections.emptyList(), Collections.emptyList());
        ShardingSphereSchema schema = new ShardingSphereSchema(Stream.of(table).collect(Collectors.toMap(ShardingSphereTable::getName, value -> value)), Collections.emptyMap());
        ColumnSegment columnSegment = createColumnSegment(null, "COL");
        Map<String, String> actual = new TablesContext(Arrays.asList(tableSegment1, tableSegment2),
                DatabaseTypeEngine.getDatabaseType("MySQL")).findTableNamesByColumnSegment(Collections.singletonList(columnSegment), schema);
        assertFalse(actual.isEmpty());
        assertThat(actual.get("col"), is("TABLE_1"));
    }
    
    @Test
    public void assertFindTableNameWhenColumnProjectionWhenSingleTable() {
        SimpleTableSegment tableSegment = createTableSegment("table_1", "tbl_1");
        ColumnProjection columnProjection = new ColumnProjection(null, "col", "cl");
        Map<String, String> actual = new TablesContext(Collections.singletonList(tableSegment), DatabaseTypeEngine.getDatabaseType("MySQL"))
                .findTableNamesByColumnProjection(Collections.singletonList(columnProjection), mock(ShardingSphereSchema.class));
        assertFalse(actual.isEmpty());
        assertThat(actual.get("col"), is("table_1"));
    }
    
    @Test
    public void assertFindTableNameWhenColumnProjectionOwnerPresent() {
        SimpleTableSegment tableSegment1 = createTableSegment("table_1", "tbl_1");
        SimpleTableSegment tableSegment2 = createTableSegment("table_2", "tbl_2");
        ColumnProjection columnProjection = new ColumnProjection("table_1", "col", "cl");
        Map<String, String> actual = new TablesContext(Arrays.asList(tableSegment1, tableSegment2), DatabaseTypeEngine.getDatabaseType("MySQL"))
                .findTableNamesByColumnProjection(Collections.singletonList(columnProjection), mock(ShardingSphereSchema.class));
        assertFalse(actual.isEmpty());
        assertThat(actual.get("table_1.col"), is("table_1"));
    }
    
    @Test
    public void assertFindTableNameWhenColumnProjectionOwnerAbsent() {
        SimpleTableSegment tableSegment1 = createTableSegment("table_1", "tbl_1");
        SimpleTableSegment tableSegment2 = createTableSegment("table_2", "tbl_2");
        ColumnProjection columnProjection = new ColumnProjection(null, "col", "cl");
        Map<String, String> actual = new TablesContext(Arrays.asList(tableSegment1, tableSegment2), DatabaseTypeEngine.getDatabaseType("MySQL"))
                .findTableNamesByColumnProjection(Collections.singletonList(columnProjection), mock(ShardingSphereSchema.class));
        assertTrue(actual.isEmpty());
    }
    
    @Test
    public void assertFindTableNameWhenColumnProjectionOwnerAbsentAndSchemaMetaDataContainsColumn() {
        SimpleTableSegment tableSegment1 = createTableSegment("table_1", "tbl_1");
        SimpleTableSegment tableSegment2 = createTableSegment("table_2", "tbl_2");
        ShardingSphereSchema schema = mock(ShardingSphereSchema.class);
        when(schema.getAllColumnNames("table_1")).thenReturn(Collections.singletonList("col"));
        ColumnProjection columnProjection = new ColumnProjection(null, "col", "cl");
        Map<String, String> actual = new TablesContext(Arrays.asList(tableSegment1, tableSegment2), DatabaseTypeEngine.getDatabaseType("MySQL"))
                .findTableNamesByColumnProjection(Collections.singletonList(columnProjection), schema);
        assertFalse(actual.isEmpty());
        assertThat(actual.get("col"), is("table_1"));
    }
    
    @Test
    public void assertFindTableNameWhenColumnProjectionOwnerAbsentAndSchemaMetaDataContainsColumnInUpperCase() {
        SimpleTableSegment tableSegment1 = createTableSegment("TABLE_1", "TBL_1");
        SimpleTableSegment tableSegment2 = createTableSegment("TABLE_2", "TBL_2");
        ShardingSphereTable table = new ShardingSphereTable("TABLE_1", Collections.singletonList(
                new ShardingSphereColumn("COL", 0, false, false, true, true, false)), Collections.emptyList(), Collections.emptyList());
        ShardingSphereSchema schema = new ShardingSphereSchema(Stream.of(table).collect(Collectors.toMap(ShardingSphereTable::getName, value -> value)), Collections.emptyMap());
        ColumnProjection columnProjection = new ColumnProjection(null, "COL", "CL");
        Map<String, String> actual = new TablesContext(Arrays.asList(tableSegment1, tableSegment2), DatabaseTypeEngine.getDatabaseType("MySQL"))
                .findTableNamesByColumnProjection(Collections.singletonList(columnProjection), schema);
        assertFalse(actual.isEmpty());
        assertThat(actual.get("col"), is("TABLE_1"));
    }
    
    private SimpleTableSegment createTableSegment(final String tableName, final String alias) {
        SimpleTableSegment result = new SimpleTableSegment(new TableNameSegment(0, 0, new IdentifierValue(tableName)));
        AliasSegment aliasSegment = new AliasSegment(0, 0, new IdentifierValue(alias));
        result.setAlias(aliasSegment);
        return result;
    }
    
    private ColumnSegment createColumnSegment(final String owner, final String name) {
        ColumnSegment result = new ColumnSegment(0, 0, new IdentifierValue(name));
        if (null != owner) {
            result.setOwner(new OwnerSegment(0, 0, new IdentifierValue(owner)));
        }
        return result;
    }
    
    @Test
    public void assertGetSchemaNameWithSameSchemaAndSameTable() {
        SimpleTableSegment tableSegment1 = createTableSegment("table_1", "tbl_1");
        tableSegment1.setOwner(new OwnerSegment(0, 0, new IdentifierValue("sharding_db_1")));
        SimpleTableSegment tableSegment2 = createTableSegment("table_1", "tbl_1");
        tableSegment2.setOwner(new OwnerSegment(0, 0, new IdentifierValue("sharding_db_1")));
        TablesContext tablesContext = new TablesContext(Arrays.asList(tableSegment1, tableSegment2), DatabaseTypeEngine.getDatabaseType("MySQL"));
        assertTrue(tablesContext.getDatabaseName().isPresent());
        assertThat(tablesContext.getDatabaseName().get(), is("sharding_db_1"));
    }
    
    @Test
    public void assertGetSchemaNameWithSameSchemaAndDifferentTable() {
        SimpleTableSegment tableSegment1 = createTableSegment("table_1", "tbl_1");
        tableSegment1.setOwner(new OwnerSegment(0, 0, new IdentifierValue("sharding_db_1")));
        SimpleTableSegment tableSegment2 = createTableSegment("table_2", "tbl_2");
        tableSegment2.setOwner(new OwnerSegment(0, 0, new IdentifierValue("sharding_db_1")));
        TablesContext tablesContext = new TablesContext(Arrays.asList(tableSegment1, tableSegment2), DatabaseTypeEngine.getDatabaseType("MySQL"));
        assertTrue(tablesContext.getDatabaseName().isPresent());
        assertThat(tablesContext.getDatabaseName().get(), is("sharding_db_1"));
    }
    
    @Test
    public void assertGetSchemaNameWithDifferentSchemaAndSameTable() {
        SimpleTableSegment tableSegment1 = createTableSegment("table_1", "tbl_1");
        tableSegment1.setOwner(new OwnerSegment(0, 0, new IdentifierValue("sharding_db_1")));
        SimpleTableSegment tableSegment2 = createTableSegment("table_1", "tbl_1");
        tableSegment2.setOwner(new OwnerSegment(0, 0, new IdentifierValue("sharding_db_2")));
        assertThrows(IllegalStateException.class, () -> new TablesContext(Arrays.asList(tableSegment1, tableSegment2), DatabaseTypeEngine.getDatabaseType("MySQL")).getDatabaseName());
    }
    
    @Test
    public void assertGetSchemaNameWithDifferentSchemaAndDifferentTable() {
        SimpleTableSegment tableSegment1 = createTableSegment("table_1", "tbl_1");
        tableSegment1.setOwner(new OwnerSegment(0, 0, new IdentifierValue("sharding_db_1")));
        SimpleTableSegment tableSegment2 = createTableSegment("table_2", "tbl_2");
        tableSegment2.setOwner(new OwnerSegment(0, 0, new IdentifierValue("sharding_db_2")));
        assertThrows(IllegalStateException.class, () -> new TablesContext(Arrays.asList(tableSegment1, tableSegment2), DatabaseTypeEngine.getDatabaseType("MySQL")).getDatabaseName());
    }
    
    @Test
    public void assertGetSchemaName() {
        SimpleTableSegment tableSegment1 = createTableSegment("table_1", "tbl_1");
        tableSegment1.setOwner(new OwnerSegment(0, 0, new IdentifierValue("sharding_db_1")));
        SimpleTableSegment tableSegment2 = createTableSegment("table_2", "tbl_2");
        tableSegment2.setOwner(new OwnerSegment(0, 0, new IdentifierValue("sharding_db_1")));
        TablesContext tablesContext = new TablesContext(Arrays.asList(tableSegment1, tableSegment2), DatabaseTypeEngine.getDatabaseType("MySQL"));
        assertTrue(tablesContext.getSchemaName().isPresent());
        assertThat(tablesContext.getSchemaName().get(), is("sharding_db_1"));
    }
    
    /***
     * 表的别名和子查询的别名大小写不同导致列名和表名对应关系建立错误
     * SELECT * FROM
     *     (SELECT T.*
     *     FROM
     *         (SELECT NAME
     *         FROM T_EMPI_PATIENT_INFO s
     *         WHERE EXISTS
     *             (SELECT 1
     *             FROM T_EMPI_PATIENT_SUSPECTED t
     *             WHERE t.PATIENT_ID = s.PATIENT_ID
     *                     AND t.ESI_ID = s.ESI_ID)
     *             ORDER BY  s.UPDATE_TIME DESC) T)
     */
     @Test
     public void assertCaseSensitivelyBetweenTableAliasAndSubqueryAlias() {
         SimpleTableSegment table1 = new SimpleTableSegment(new TableNameSegment(48, 66, new IdentifierValue("T_EMPI_PATIENT_INFO")));
         table1.setOwner(null);
         table1.setAlias(new AliasSegment(68, 68, new IdentifierValue("s")));
         SimpleTableSegment table2 = new SimpleTableSegment(new TableNameSegment(98, 121, new IdentifierValue("T_EMPI_PATIENT_SUSPECTED")));
         table2.setOwner(null);
         table2.setAlias(new AliasSegment(123, 123, new IdentifierValue("t")));
         Collection<SimpleTableSegment> tables = new LinkedList<>();
         tables.add(table1);
         tables.add(table2);
         TablesContext tablesContext = new TablesContext(tables, null);
         Collection<ColumnProjection> columns = new LinkedList<>();
         columns.add(new ColumnProjection("T", "NAME", null));
         Map<String, Collection<String>> ownerColumnNames = tablesContext.getOwnerColumnNamesByColumnProjection(columns);
         /***
          * findTableNameFromSQL根据列名和所属关系查找对应的表
          * 错误的列和表对应关系是 T.NAME -> T_EMPI_PATIENT_SUSPECTED
          * 正确的列和表对应关系应该是 result中不包含T.NAME对应的表名
          */
         Map<String, String> result = tablesContext.findTableNameFromSQL(ownerColumnNames);
         assertEquals(false, result.containsKey("T_EMPI_PATIENT_SUSPECTED"), "表的别名和子查询的别名大小写不同导致列名和表名对应关系建立错误");
     }
}
