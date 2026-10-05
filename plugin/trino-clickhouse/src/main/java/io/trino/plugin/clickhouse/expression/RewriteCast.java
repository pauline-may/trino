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
package io.trino.plugin.clickhouse.expression;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.expression.AbstractRewriteCast;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;

import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;

/**
 * Pushes down widening casts between integer types. Narrowing casts are not pushed down, because ClickHouse wraps
 * values that do not fit in the target type, whereas Trino fails.
 */
public class RewriteCast
        extends AbstractRewriteCast
{
    private static final List<Type> INTEGER_TYPES = ImmutableList.of(TINYINT, SMALLINT, INTEGER, BIGINT);
    private static final Map<Type, JdbcTypeHandle> TARGET_TYPES = ImmutableMap.of(
            SMALLINT, typeHandle(Types.SMALLINT, ClickHouseDataType.Int16),
            INTEGER, typeHandle(Types.INTEGER, ClickHouseDataType.Int32),
            BIGINT, typeHandle(Types.BIGINT, ClickHouseDataType.Int64));

    public RewriteCast(BiFunction<ConnectorSession, Type, String> jdbcTypeProvider)
    {
        super(jdbcTypeProvider);
    }

    @Override
    protected Optional<JdbcTypeHandle> toJdbcTypeHandle(JdbcTypeHandle sourceType, Type targetType)
    {
        Optional<Type> sourceTrinoType = sourceType.jdbcTypeName().flatMap(RewriteCast::integerType);
        if (sourceTrinoType.isEmpty() || !TARGET_TYPES.containsKey(targetType)) {
            return Optional.empty();
        }
        if (INTEGER_TYPES.indexOf(targetType) <= INTEGER_TYPES.indexOf(sourceTrinoType.get())) {
            return Optional.empty();
        }
        return Optional.of(TARGET_TYPES.get(targetType));
    }

    @Override
    protected String buildCast(Type sourceType, Type targetType, String expression, String castType)
    {
        // Nullable, so that NULL values are preserved and an aggregation over empty input returns NULL
        return "CAST(%s AS Nullable(%s))".formatted(expression, castType);
    }

    // Same mapping as in ClickHouseClient. UInt32 is mapped to bigint, which has no wider integer type.
    private static Optional<Type> integerType(String clickHouseType)
    {
        return switch (ClickHouseColumn.of("", clickHouseType).getDataType()) {
            case Int8 -> Optional.of(TINYINT);
            case Int16, UInt8 -> Optional.of(SMALLINT);
            case Int32, UInt16 -> Optional.of(INTEGER);
            default -> Optional.empty();
        };
    }

    private static JdbcTypeHandle typeHandle(int jdbcType, ClickHouseDataType dataType)
    {
        return new JdbcTypeHandle(jdbcType, Optional.of(dataType.name()), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }
}
