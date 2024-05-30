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

package org.apache.shardingsphere.infra.binder.segment.select.pagination.engine;

import org.apache.shardingsphere.infra.binder.segment.select.pagination.PaginationContext;
import org.apache.shardingsphere.infra.binder.segment.select.projection.Projection;
import org.apache.shardingsphere.infra.binder.segment.select.projection.ProjectionsContext;
import org.apache.shardingsphere.infra.binder.segment.select.projection.impl.ColumnProjection;
import org.apache.shardingsphere.sql.parser.sql.common.enums.QuoteCharacter;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.column.ColumnSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.expr.BinaryOperationExpression;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.expr.ExpressionSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.expr.FunctionSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.expr.simple.LiteralExpressionSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.expr.simple.ParameterMarkerExpressionSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.pagination.PaginationValueSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.pagination.rownum.NumberLiteralRowNumberValueSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.pagination.rownum.ParameterMarkerRowNumberValueSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.predicate.AndPredicate;
import org.apache.shardingsphere.sql.parser.sql.common.value.identifier.IdentifierValue;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

public final class RowNumberPaginationContextEngineTest {
    
    private static final String ROW_NUMBER_COLUMN_NAME = "rownum";
    
    private static final String ROW_NUMBER_COLUMN_ALIAS = "predicateRowNumberAlias";
    
    @Test
    public void assertCreatePaginationContextWhenRowNumberAliasNotPresent() {
        ProjectionsContext projectionsContext = new ProjectionsContext(0, 0, false, Collections.emptyList());
        PaginationContext paginationContext = new RowNumberPaginationContextEngine().createPaginationContext(Collections.emptyList(), projectionsContext, Collections.emptyList());
        assertFalse(paginationContext.getOffsetSegment().isPresent());
        assertFalse(paginationContext.getRowCountSegment().isPresent());
    }
    
    @Test
    public void assertCreatePaginationContextWhenRowNumberAliasIsPresentAndRowNumberPredicatesIsEmpty() {
        Projection projectionWithRowNumberAlias = new ColumnProjection(null, ROW_NUMBER_COLUMN_NAME, ROW_NUMBER_COLUMN_ALIAS);
        ProjectionsContext projectionsContext = new ProjectionsContext(0, 0, false, Collections.singleton(projectionWithRowNumberAlias));
        PaginationContext paginationContext = new RowNumberPaginationContextEngine().createPaginationContext(Collections.emptyList(), projectionsContext, Collections.emptyList());
        assertFalse(paginationContext.getOffsetSegment().isPresent());
        assertFalse(paginationContext.getRowCountSegment().isPresent());
    }
    
    @Test
    public void assertCreatePaginationContextWhenRowNumberAliasIsPresentAndRowNumberPredicatesNotEmptyWithLessThanOperator() {
        assertCreatePaginationContextWhenRowNumberAliasPresentAndRowNumberPredicatedNotEmptyWithGivenOperator("<");
    }
    
    @Test
    public void assertCreatePaginationContextWhenRowNumberAliasIsPresentAndRowNumberPredicatesNotEmptyWithLessThanEqualOperator() {
        assertCreatePaginationContextWhenRowNumberAliasPresentAndRowNumberPredicatedNotEmptyWithGivenOperator("<=");
    }
    
    @Test
    public void assertCreatePaginationContextWhenOffsetSegmentInstanceOfNumberLiteralRowNumberValueSegmentWithGreatThanOperator() {
        assertCreatePaginationContextWhenOffsetSegmentInstanceOfNumberLiteralRowNumberValueSegmentWithGivenOperator(">");
    }
    
    @Test
    public void assertCreatePaginationContextWhenOffsetSegmentInstanceOfNumberLiteralRowNumberValueSegmentWithGreatThanEqualOperator() {
        assertCreatePaginationContextWhenOffsetSegmentInstanceOfNumberLiteralRowNumberValueSegmentWithGivenOperator(">=");
    }
    
    @Test
    public void assertCreatePaginationContextWhenRowNumberAliasIsPresentAndRowNumberPredicatesNotEmpty() {
        Projection projectionWithRowNumberAlias = new ColumnProjection(null, ROW_NUMBER_COLUMN_NAME, ROW_NUMBER_COLUMN_ALIAS);
        ProjectionsContext projectionsContext = new ProjectionsContext(0, 0, false, Collections.singleton(projectionWithRowNumberAlias));
        AndPredicate andPredicate = new AndPredicate();
        ColumnSegment left = new ColumnSegment(0, 10, new IdentifierValue(ROW_NUMBER_COLUMN_NAME));
        BinaryOperationExpression predicateSegment = new BinaryOperationExpression(0, 0, left, null, null, null);
        andPredicate.getPredicates().add(predicateSegment);
        PaginationContext paginationContext = new RowNumberPaginationContextEngine().createPaginationContext(Collections.emptyList(), projectionsContext, Collections.emptyList());
        assertFalse(paginationContext.getOffsetSegment().isPresent());
        assertFalse(paginationContext.getRowCountSegment().isPresent());
    }
    
    @Test
    public void assertCreatePaginationContextWhenParameterMarkerRowNumberValueSegment() {
        Projection projectionWithRowNumberAlias = new ColumnProjection(null, ROW_NUMBER_COLUMN_NAME, ROW_NUMBER_COLUMN_ALIAS);
        ProjectionsContext projectionsContext = new ProjectionsContext(0, 0, false, Collections.singleton(projectionWithRowNumberAlias));
        ColumnSegment left = new ColumnSegment(0, 10, new IdentifierValue(ROW_NUMBER_COLUMN_NAME));
        ParameterMarkerExpressionSegment right = new ParameterMarkerExpressionSegment(0, 10, 0);
        BinaryOperationExpression expression = new BinaryOperationExpression(0, 0, left, right, ">", null);
        PaginationContext paginationContext = new RowNumberPaginationContextEngine()
                .createPaginationContext(Collections.singletonList(expression), projectionsContext, Collections.singletonList(1));
        Optional<PaginationValueSegment> offSetSegmentPaginationValue = paginationContext.getOffsetSegment();
        assertTrue(offSetSegmentPaginationValue.isPresent());
        assertThat(offSetSegmentPaginationValue.get(), instanceOf(ParameterMarkerRowNumberValueSegment.class));
        assertFalse(paginationContext.getRowCountSegment().isPresent());
    }

    /***
     * SELECT ROWNUM AS RN FROM T_EMPI_PATIENT_INDEX WHERE ROWNUM <= to_number('1') * to_number('10')
     * this sql execute throw java.lang.ClassCastException, BinaryOperationExpression cannot be cast ParameterMarkerExpressionSegment
     */
    @Test
    public void assertCreatePaginationWithRowNumberTest() {
        ColumnSegment left = new ColumnSegment(52, 57, new IdentifierValue("ROWNUM", QuoteCharacter.NONE));
        FunctionSegment leftFunction = new FunctionSegment(62, 75, "to_number", "to_number('1')");
        leftFunction.setOwner(null);
        FunctionSegment rightFunction = new FunctionSegment(79, 93, "to_number", "to_number('10')");
        rightFunction.setOwner(null);
        BinaryOperationExpression right = new BinaryOperationExpression(62, 93, leftFunction, rightFunction, "*", "to_number('1') * to_number('10')");
        BinaryOperationExpression expression = new BinaryOperationExpression(52, 93, left, right, "<=", "ROWNUM <= to_number('1') * to_number('10')");
        Collection<ExpressionSegment> expressions = new LinkedList<>();
        expressions.add(expression);

        Collection<Projection> projections = new LinkedList<>();
        projections.add(new ColumnProjection(null, "ROWNUM", "RN"));
        ProjectionsContext projectionsContext = new ProjectionsContext(7, 18, false, projections);
        final List<Object> params = new LinkedList<>();

        assertNotNull(new RowNumberPaginationContextEngine().createPaginationContext(expressions, projectionsContext, params));
    }

    
    private void assertCreatePaginationContextWhenRowNumberAliasPresentAndRowNumberPredicatedNotEmptyWithGivenOperator(final String operator) {
        Projection projectionWithRowNumberAlias = new ColumnProjection(null, ROW_NUMBER_COLUMN_NAME, ROW_NUMBER_COLUMN_ALIAS);
        ProjectionsContext projectionsContext = new ProjectionsContext(0, 0, false, Collections.singleton(projectionWithRowNumberAlias));
        ColumnSegment left = new ColumnSegment(0, 10, new IdentifierValue(ROW_NUMBER_COLUMN_NAME));
        LiteralExpressionSegment right = new LiteralExpressionSegment(0, 10, 100);
        BinaryOperationExpression expression = new BinaryOperationExpression(0, 0, left, right, operator, null);
        PaginationContext paginationContext = new RowNumberPaginationContextEngine().createPaginationContext(Collections.singletonList(expression), projectionsContext, Collections.emptyList());
        assertFalse(paginationContext.getOffsetSegment().isPresent());
        Optional<PaginationValueSegment> paginationValueSegment = paginationContext.getRowCountSegment();
        assertTrue(paginationValueSegment.isPresent());
        NumberLiteralRowNumberValueSegment numberLiteralRowNumberValueSegment = (NumberLiteralRowNumberValueSegment) paginationValueSegment.get();
        assertThat(numberLiteralRowNumberValueSegment.getStartIndex(), is(0));
        assertThat(numberLiteralRowNumberValueSegment.getStopIndex(), is(10));
        assertThat(numberLiteralRowNumberValueSegment.getValue(), is(100L));
    }
    
    private void assertCreatePaginationContextWhenOffsetSegmentInstanceOfNumberLiteralRowNumberValueSegmentWithGivenOperator(final String operator) {
        Projection projectionWithRowNumberAlias = new ColumnProjection(null, ROW_NUMBER_COLUMN_NAME, ROW_NUMBER_COLUMN_ALIAS);
        ProjectionsContext projectionsContext = new ProjectionsContext(0, 0, false, Collections.singleton(projectionWithRowNumberAlias));
        ColumnSegment left = new ColumnSegment(0, 10, new IdentifierValue(ROW_NUMBER_COLUMN_NAME));
        LiteralExpressionSegment right = new LiteralExpressionSegment(0, 10, 100);
        BinaryOperationExpression expression = new BinaryOperationExpression(0, 0, left, right, operator, null);
        PaginationContext rowNumberPaginationContextEngine = new RowNumberPaginationContextEngine()
                .createPaginationContext(Collections.singletonList(expression), projectionsContext, Collections.emptyList());
        Optional<PaginationValueSegment> paginationValueSegment = rowNumberPaginationContextEngine.getOffsetSegment();
        assertTrue(paginationValueSegment.isPresent());
        PaginationValueSegment actualPaginationValueSegment = paginationValueSegment.get();
        assertThat(actualPaginationValueSegment, instanceOf(NumberLiteralRowNumberValueSegment.class));
        assertThat(actualPaginationValueSegment.getStartIndex(), is(0));
        assertThat(actualPaginationValueSegment.getStopIndex(), is(10));
        assertThat(((NumberLiteralRowNumberValueSegment) actualPaginationValueSegment).getValue(), is(100L));
        assertFalse(rowNumberPaginationContextEngine.getRowCountSegment().isPresent());
    }
}
