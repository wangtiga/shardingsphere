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

package org.apache.shardingsphere.sql.parser.core.database.parser;

import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.apache.shardingsphere.infra.util.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.sql.parser.api.parser.SQLParser;
import org.apache.shardingsphere.sql.parser.core.ParseASTNode;
import org.apache.shardingsphere.sql.parser.core.SQLParserFactory;
import org.apache.shardingsphere.sql.parser.exception.SQLParsingException;
import org.apache.shardingsphere.sql.parser.spi.DatabaseTypedSQLParserFacade;

import java.util.ArrayList;

/**
 * SQL parser executor.
 */
@RequiredArgsConstructor
public final class SQLParserExecutor {
    
    private final String databaseType;
    
    /**
     * Parse SQL.
     * 
     * @param sql SQL to be parsed
     * @return parse AST node
     */
    public ParseASTNode parse(final String sql) {
        ParseASTNode result = twoPhaseParse(sql);
        if (result.getRootNode() instanceof ErrorNode) {
            throw new SQLParsingException(sql);
        }
        return result;
    }
    
    private ParseASTNode twoPhaseParse(final String sql) {
        String decorateSql = decorateKeyWord(sql);
        DatabaseTypedSQLParserFacade sqlParserFacade = TypedSPILoader.getService(DatabaseTypedSQLParserFacade.class, databaseType);
        SQLParser sqlParser = SQLParserFactory.newInstance(decorateSql, sqlParserFacade.getLexerClass(), sqlParserFacade.getParserClass());
        try {
            ((Parser) sqlParser).getInterpreter().setPredictionMode(PredictionMode.SLL);
            return (ParseASTNode) sqlParser.parse();
        } catch (final ParseCancellationException ex) {
            ((Parser) sqlParser).reset();
            ((Parser) sqlParser).getInterpreter().setPredictionMode(PredictionMode.LL);
            ((Parser) sqlParser).removeErrorListeners();
            ((Parser) sqlParser).addErrorListener(SQLParserErrorListener.getInstance());
            try {
                return (ParseASTNode) sqlParser.parse();
            } catch (final ParseCancellationException e) {
                throw new SQLParsingException(decorateSql + ", " + e.getMessage());
            }
        }
    }

    // 修饰保留关键字LEVEL作为表字段
    private static String decorateKeyWord(String sql) {
        String sqlUpper = sql.toUpperCase();
        ArrayList<Integer> levelStartIndexs = new ArrayList<>();
        int fromIndex = 0;
        while (fromIndex < sqlUpper.length()) {
            fromIndex = sqlUpper.indexOf("LEVEL", fromIndex);
            if (fromIndex == -1) {
                break;
            }
            levelStartIndexs.add(fromIndex);
            fromIndex += 5;
        }
        if (levelStartIndexs.isEmpty()) {
            return sql;
        }

        StringBuilder stringBuilder = new StringBuilder();
        int len = sqlUpper.length();
        stringBuilder.append(sqlUpper.substring(0, levelStartIndexs.get(0)));
        for (int i = 0; i < levelStartIndexs.size(); ++i) {
            int levelStartIndex = levelStartIndexs.get(i);
            boolean isPureKeyWord = false;
            if (levelStartIndex - 1 >= 0 && levelStartIndex + 5 < len) {
                // " LEVEL "
                if (sqlUpper.charAt(levelStartIndex - 1) == ' ' && sqlUpper.charAt(levelStartIndex + 5) == ' ') {
                    stringBuilder.append("\"LEVEL\"");
                    isPureKeyWord = true;
                } else if (sqlUpper.charAt(levelStartIndex - 1) == ' ' && sqlUpper.charAt(levelStartIndex + 5) == ',') {
                    stringBuilder.append("\"LEVEL\"");
                    isPureKeyWord = true;
                } else if (sqlUpper.charAt(levelStartIndex - 1) == ',' && sqlUpper.charAt(levelStartIndex + 5) == ',') {
                    stringBuilder.append("\"LEVEL\"");
                    isPureKeyWord = true;
                } else if (sqlUpper.charAt(levelStartIndex - 1) == ',' && sqlUpper.charAt(levelStartIndex + 5) == ' ') {
                    stringBuilder.append("\"LEVEL\"");
                    isPureKeyWord = true;
                }
            }

            if (i + 1 < levelStartIndexs.size()) {
                if (isPureKeyWord) {
                    stringBuilder.append(sqlUpper.substring(levelStartIndex + 5, levelStartIndexs.get(i + 1)));
                } else {
                    stringBuilder.append(sqlUpper.substring(levelStartIndex, levelStartIndexs.get(i + 1)));
                }
            } else {
                if (isPureKeyWord) {
                    stringBuilder.append(sqlUpper.substring(levelStartIndex + 5));
                } else {
                    stringBuilder.append(sqlUpper.substring(levelStartIndex));
                }
            }
        }
        return stringBuilder.toString();
    }
}
