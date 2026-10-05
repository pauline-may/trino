/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.clickhouse;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcCastPushdownTest;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.plugin.clickhouse.ClickHouseQueryRunner.TPCH_SCHEMA;
import static io.trino.plugin.clickhouse.TestingClickHouseServer.CLICKHOUSE_LATEST_IMAGE;
import static org.assertj.core.api.Assertions.assertThat;

final class TestClickHouseCastPushdown
        extends BaseJdbcCastPushdownTest
{
    private TestingClickHouseServer clickHouseServer;

    private TestTable left;
    private TestTable right;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        clickHouseServer = closeAfterClass(new TestingClickHouseServer(CLICKHOUSE_LATEST_IMAGE));
        return ClickHouseQueryRunner.builder(clickHouseServer).build();
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return clickHouseServer::execute;
    }

    @BeforeAll
    void setupTable()
    {
        left = closeAfterClass(new TestTable(
                onRemoteDatabase(),
                TPCH_SCHEMA + ".left_table_",
                """
                (
                id Int64,
                c_int8 Nullable(Int8),
                c_int16 Nullable(Int16),
                c_int32 Nullable(Int32),
                c_int64 Nullable(Int64),
                c_uint8 Nullable(UInt8),
                c_uint16 Nullable(UInt16),
                c_uint32 Nullable(UInt32),
                c_int32_not_null Int32,
                c_int16_out_of_range Nullable(Int16),
                c_int32_out_of_range Nullable(Int32),
                c_int64_out_of_range Nullable(Int64),
                c_uint8_out_of_range Nullable(UInt8),
                c_float64 Nullable(Float64),
                c_decimal_10_2 Nullable(Decimal(10, 2))
                ) Engine=Log""",
                ImmutableList.of(
                        "11, 1, 1, 1, 1, 1, 1, 1, 1, 300, 70000, 3000000000, 200, 1.5, 1.23",
                        "12, -2, -2, -2, -2, 2, 2, 2, 2, -300, -70000, -3000000000, 255, -2.5, -2.67",
                        "13, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 3, NULL, NULL, NULL, NULL, NULL, NULL")));
        right = closeAfterClass(new TestTable(
                onRemoteDatabase(),
                TPCH_SCHEMA + ".right_table_",
                "(id Int64, c_int8 Nullable(Int8), c_int16 Nullable(Int16), c_int32 Nullable(Int32), c_int64 Nullable(Int64)) Engine=Log",
                ImmutableList.of(
                        "21, 1, 1, 1, 1",
                        "22, 2, 2, 2, 2",
                        "23, NULL, NULL, NULL, NULL")));
    }

    @Override
    protected String leftTable()
    {
        return left.getName();
    }

    @Override
    protected String rightTable()
    {
        return right.getName();
    }

    @Test
    @Override
    public void testJoinPushdownWithCast()
    {
        // join pushdown is not supported
        for (CastTestCase testCase : supportedCastTypePushdown()) {
            assertThat(query("SELECT l.id FROM %s l JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                    .joinIsNotFullyPushedDown();
        }
    }

    @Test
    void testCastPushdownDisabled()
    {
        Session sessionWithoutPushdown = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "complex_expression_pushdown", "false")
                .build();
        assertThat(query(sessionWithoutPushdown, "SELECT CAST(c_int32 AS bigint) FROM %s".formatted(leftTable())))
                .isNotFullyPushedDown(ProjectNode.class);
    }

    @Override
    protected List<CastTestCase> supportedCastTypePushdown()
    {
        return ImmutableList.<CastTestCase>builder()
                .add(new CastTestCase("c_int8", "smallint", "c_int16"))
                .add(new CastTestCase("c_int8", "integer", "c_int32"))
                .add(new CastTestCase("c_int8", "bigint", "c_int64"))
                .add(new CastTestCase("c_int16", "integer", "c_int32"))
                .add(new CastTestCase("c_int16", "bigint", "c_int64"))
                .add(new CastTestCase("c_int32", "bigint", "c_int64"))
                .add(new CastTestCase("c_uint8", "integer", "c_int32"))
                .add(new CastTestCase("c_uint8", "bigint", "c_int64"))
                .add(new CastTestCase("c_uint16", "bigint", "c_int64"))
                .add(new CastTestCase("c_int32_not_null", "bigint", "c_int64"))
                .build();
    }

    @Override
    protected List<CastTestCase> unsupportedCastTypePushdown()
    {
        return ImmutableList.<CastTestCase>builder()
                // narrowing
                .add(new CastTestCase("c_int16", "tinyint", "c_int8"))
                .add(new CastTestCase("c_int32", "smallint", "c_int16"))
                .add(new CastTestCase("c_int64", "integer", "c_int32"))
                .add(new CastTestCase("c_uint8", "tinyint", "c_int8"))
                .add(new CastTestCase("c_uint32", "integer", "c_int32"))
                // not an integer
                .add(new CastTestCase("c_float64", "bigint", "c_int64"))
                .add(new CastTestCase("c_decimal_10_2", "bigint", "c_int64"))
                .build();
    }

    @Override
    protected List<InvalidCastTestCase> invalidCast()
    {
        return ImmutableList.<InvalidCastTestCase>builder()
                .add(new InvalidCastTestCase("c_int16_out_of_range", "tinyint", "Out of range for tinyint: -?300"))
                .add(new InvalidCastTestCase("c_int32_out_of_range", "smallint", "Out of range for smallint: -?70000"))
                .add(new InvalidCastTestCase("c_int64_out_of_range", "integer", "Out of range for integer: -?3000000000"))
                .add(new InvalidCastTestCase("c_uint8_out_of_range", "tinyint", "Out of range for tinyint: (200|255)"))
                .build();
    }
}
