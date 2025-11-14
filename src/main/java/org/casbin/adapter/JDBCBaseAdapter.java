// Copyright 2018 The casbin Authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.casbin.adapter;

import dev.failsafe.ExecutionContext;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import org.apache.commons.collections4.CollectionUtils;
import org.casbin.jcasbin.exception.CasbinAdapterException;
import org.casbin.jcasbin.model.Assertion;
import org.casbin.jcasbin.model.Model;
import org.casbin.jcasbin.persist.Adapter;
import org.casbin.jcasbin.persist.BatchAdapter;
import org.casbin.jcasbin.persist.Helper;
import org.casbin.jcasbin.persist.UpdatableAdapter;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Duration;
import java.util.*;

class CasbinRule {
    int id; //Fields reserved for compatibility with other adapters, and the primary key is automatically incremented.
    String ptype;
    String v0;
    String v1;
    String v2;
    String v3;
    String v4;
    String v5;

    public String[] toStringArray() {
        return new String[]{ptype, v0, v1, v2, v3, v4, v5};
    }
}

/**
 * JDBCAdapter is the JDBC adapter for jCasbin.
 * It can load policy from JDBC supported database or save policy to it.
 */
abstract class JDBCBaseAdapter implements Adapter, BatchAdapter, UpdatableAdapter {
    protected static final String DEFAULT_TABLE_NAME = "casbin_rule";
    protected static final boolean DEFAULT_REMOVE_POLICY_FAILED = false;
    protected static final boolean DEFAULT_AUTO_CREATE_TABLE = true;
    protected static final int _DEFAULT_CONNECTION_TRIES = 3;
    protected DataSource dataSource;
    protected String tableName;
    protected boolean removePolicyFailed;
    private final int batchSize = 1000;
    protected Connection conn;
    protected RetryPolicy<Object> retryPolicy;

    /**
     * JDBCAdapter is the constructor for JDBCAdapter.
     *
     * @param driver   the JDBC driver, like "com.mysql.cj.jdbc.Driver".
     * @param url      the JDBC URL, like "jdbc:mysql://localhost:3306/casbin".
     * @param username the username of the database.
     * @param password the password of the database.
     */
    protected JDBCBaseAdapter(String driver, String url, String username, String password) throws Exception {
        this(new JDBCDataSource(driver, url, username, password));
    }

    protected JDBCBaseAdapter(String driver, String url, String username, String password, boolean removePolicyFailed, String tableName, boolean autoCreateTable) throws Exception {
        this(new JDBCDataSource(driver, url, username, password), removePolicyFailed, tableName, autoCreateTable);
    }

    /**
     * The constructor for JDBCAdapter, will not try to create database.
     *
     * @param dataSource the JDBC DataSource.
     */
    protected JDBCBaseAdapter(DataSource dataSource) throws Exception {
        this(dataSource, DEFAULT_REMOVE_POLICY_FAILED, DEFAULT_TABLE_NAME, DEFAULT_AUTO_CREATE_TABLE);
    }

    protected JDBCBaseAdapter(DataSource dataSource, boolean removePolicyFailed, String tableName, boolean autoCreateTable) throws Exception {
        this.dataSource = dataSource;
        this.tableName = tableName;
        this.removePolicyFailed = removePolicyFailed;
        retryPolicy = RetryPolicy.builder()
            .handle(SQLException.class)
            .withDelay(Duration.ofSeconds(1))
            .withMaxRetries(_DEFAULT_CONNECTION_TRIES)
            .build();
        conn = dataSource.getConnection();
        if (autoCreateTable) {
            migrate();
        }
    }

    protected void migrate() throws SQLException {
        Statement stmt = conn.createStatement();
        String sql = renderActualSql("CREATE TABLE IF NOT EXISTS casbin_rule(id int NOT NULL PRIMARY KEY auto_increment, ptype VARCHAR(100) NOT NULL, v0 VARCHAR(100), v1 VARCHAR(100), v2 VARCHAR(100), v3 VARCHAR(100), v4 VARCHAR(100), v5 VARCHAR(100))");
        String productName = conn.getMetaData().getDatabaseProductName();

        switch (productName) {
            case "MySQL":
                String hasTableSql = renderActualSql("SHOW TABLES LIKE 'casbin_rule';");
                ResultSet rs = stmt.executeQuery(hasTableSql);
                if (rs.next()) {
                    return;
                }
                break;
            case "Oracle":
                sql = renderActualSql("declare begin execute immediate 'CREATE TABLE CASBIN_RULE(id NUMBER(5, 0) not NULL primary key, ptype VARCHAR(100) not NULL, v0 VARCHAR(100), v1 VARCHAR(100), v2 VARCHAR(100), v3 VARCHAR(100), v4 VARCHAR(100), v5 VARCHAR(100))'; " +
                        "exception when others then " +
                        "if SQLCODE = -955 then " +
                        "null; " +
                        "else raise; " +
                        "end if; " +
                        "end;");
                break;
            case "Microsoft SQL Server":
                sql = renderActualSql("IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='casbin_rule' and xtype='U') CREATE TABLE casbin_rule(id int NOT NULL primary key identity(1, 1), ptype VARCHAR(100) NOT NULL, v0 VARCHAR(100), v1 VARCHAR(100), v2 VARCHAR(100), v3 VARCHAR(100), v4 VARCHAR(100), v5 VARCHAR(100))");
                break;
            case "PostgreSQL":
                sql = renderActualSql("do $$ " +
                        "BEGIN " +
                        "IF (select count(*) from information_schema.tables where table_name = 'casbin_rule') = 0 " +
                        "THEN " +
                        "CREATE SEQUENCE IF NOT EXISTS CASBIN_SEQUENCE START 1; "+
                        "END IF; " +
                        "END; " +
                        "$$;");
                break;
            case "H2":
                sql = renderActualSql("CREATE TABLE IF NOT EXISTS casbin_rule(id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY, ptype VARCHAR(100) NOT NULL, v0 VARCHAR(100), v1 VARCHAR(100), v2 VARCHAR(100), v3 VARCHAR(100), v4 VARCHAR(100), v5 VARCHAR(100))");
                break;
        }

        stmt.executeUpdate(sql);
        if ("Oracle".equals(productName)) {
            sql = renderActualSql("declare " +
                    "V_NUM number;" +
                    "BEGIN " +
                    "V_NUM := 0;  " +
                    "select count(0) into V_NUM from user_sequences where sequence_name = 'CASBIN_SEQUENCE';" +
                    "if V_NUM > 0 then " +
                    "null;" +
                    "else " +
                    "execute immediate 'CREATE SEQUENCE casbin_sequence increment by 1 start with 1 nomaxvalue nocycle nocache';" +
                    "end if;END;");
            stmt.executeUpdate(sql);
            sql = renderActualSql("declare " +
                    "V_NUM number;" +
                    "BEGIN " +
                    "V_NUM := 0;" +
                    "select count(0) into V_NUM from user_triggers where trigger_name = 'CASBIN_ID_AUTOINCREMENT';" +
                    "if V_NUM > 0 then " +
                    "null;" +
                    "else " +
                    "execute immediate 'create trigger casbin_id_autoincrement before " +
                    "                        insert on CASBIN_RULE for each row " +
                    "                        when (new.id is null) " +
                    "                        begin " +
                    "                        select casbin_sequence.nextval into:new.id from dual;" +
                    "                        end;';" +
                    "end if;" +
                    "END;");
            stmt.executeUpdate(sql);
        } else if ("PostgreSQL".equals(productName)) {
            sql = renderActualSql("CREATE TABLE IF NOT EXISTS casbin_rule(id int NOT NULL PRIMARY KEY default nextval('CASBIN_SEQUENCE'::regclass), ptype VARCHAR(100) NOT NULL, v0 VARCHAR(100), v1 VARCHAR(100), v2 VARCHAR(100), v3 VARCHAR(100), v4 VARCHAR(100), v5 VARCHAR(100))");
            stmt.executeUpdate(sql);
        }
    }

    protected void loadPolicyLine(CasbinRule line, Model model) {
        escapeCasbinRule(line);
        String lineText = line.ptype;
        if (!"".equals(line.v0)) {
            lineText += ", " + line.v0;
        }
        if (!"".equals(line.v1)) {
            lineText += ", " + line.v1;
        }
        if (!"".equals(line.v2)) {
            lineText += ", " + line.v2;
        }
        if (!"".equals(line.v3)) {
            lineText += ", " + line.v3;
        }
        if (!"".equals(line.v4)) {
            lineText += ", " + line.v4;
        }
        if (!"".equals(line.v5)) {
            lineText += ", " + line.v5;
        }

        Helper.loadPolicyLine(lineText, model);
    }

    /**
     * loadPolicy loads all policy rules from the storage.
     */
    @Override
    public void loadPolicy(Model model) {
        Failsafe.with(retryPolicy).run(ctx -> {
            if (ctx.isRetry()) {
                retry(ctx);
            }
            try (Statement stmt = conn.createStatement();
                 ResultSet rSet = stmt.executeQuery(renderActualSql("SELECT ptype,v0,v1,v2,v3,v4,v5 FROM casbin_rule"))) {
                ResultSetMetaData rData = rSet.getMetaData();
                while (rSet.next()) {
                    CasbinRule line = new CasbinRule();
                    line.ptype = rSet.getObject(1) == null ? "" : (String) rSet.getObject(1);
                    line.v0 = rSet.getObject(2) == null ? "" : (String) rSet.getObject(2);
                    line.v1 = rSet.getObject(3) == null ? "" : (String) rSet.getObject(3);
                    line.v2 = rSet.getObject(4) == null ? "" : (String) rSet.getObject(4);
                    line.v3 = rSet.getObject(5) == null ? "" : (String) rSet.getObject(5);
                    line.v4 = rSet.getObject(6) == null ? "" : (String) rSet.getObject(6);
                    line.v5 = rSet.getObject(7) == null ? "" : (String) rSet.getObject(7);
                    loadPolicyLine(line, model);
                }
            }
        });
    }

    private CasbinRule savePolicyLine(String ptype, List<String> rule) {
        CasbinRule line = new CasbinRule();

        line.ptype = ptype;
        if (rule.size() > 0) {
            line.v0 = rule.get(0);
        }
        if (rule.size() > 1) {
            line.v1 = rule.get(1);
        }
        if (rule.size() > 2) {
            line.v2 = rule.get(2);
        }
        if (rule.size() > 3) {
            line.v3 = rule.get(3);
        }
        if (rule.size() > 4) {
            line.v4 = rule.get(4);
        }
        if (rule.size() > 5) {
            line.v5 = rule.get(5);
        }

        return line;
    }

    /**
     * savePolicy saves all policy rules to the storage.
     */
    @Override
    public void savePolicy(Model model) {
        String cleanSql = renderActualSql("delete from casbin_rule");
        String addSql = renderActualSql("INSERT INTO casbin_rule (ptype,v0,v1,v2,v3,v4,v5) VALUES(?,?,?,?,?,?,?)");

        Failsafe.with(retryPolicy).run(ctx -> {
            if (ctx.isRetry()) {
                retry(ctx);
            }
            conn.setAutoCommit(false);

            int count = 0;

            try (Statement statement = conn.createStatement(); PreparedStatement ps = conn.prepareStatement(addSql)) {
                statement.execute(cleanSql);
                count = saveSectionPolicyWithBatch(model, "p", ps, count);
                count = saveSectionPolicyWithBatch(model, "g", ps, count);

                if (count != 0) {
                    ps.executeBatch();
                }

                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        });
    }

    /**
     * saveSectionPolicyWithBatch saves section policy rules to the storage.
     * as a helper function for savePolicy
     * only when the batchCount exceeds the batchSize will the actual save operation be performed
     */
    private int saveSectionPolicyWithBatch(Model model, String section, PreparedStatement ps, int batchCount) throws SQLException {
        if (!model.model.containsKey(section)) return batchCount;
        for (Map.Entry<String, Assertion> entry : model.model.get(section).entrySet()) {
            String ptype = entry.getKey();
            Assertion ast = entry.getValue();

            for (List<String> rule : ast.policy) {
                CasbinRule line = savePolicyLine(ptype, rule);

                ps.setString(1, line.ptype);
                ps.setString(2, line.v0);
                ps.setString(3, line.v1);
                ps.setString(4, line.v2);
                ps.setString(5, line.v3);
                ps.setString(6, line.v4);
                ps.setString(7, line.v5);

                ps.addBatch();
                if (++batchCount == batchSize) {
                    batchCount = 0;
                    ps.executeBatch();
                    ps.clearBatch();
                }
            }
        }
        return batchCount;
    }

    /**
     * addPolicy adds a policy rule to the storage.
     */
    @Override
    public void addPolicy(String sec, String ptype, List<String> rule) {
        List<List<String>> rules = new ArrayList<>();
        rules.add(rule);
        this.addPolicies(sec, ptype, rules);
    }

    @Override
    public void addPolicies(String sec, String ptype, List<List<String>> rules) {
        if (CollectionUtils.isEmpty(rules)) {
            return;
        }

        String sql = renderActualSql("INSERT INTO casbin_rule (ptype,v0,v1,v2,v3,v4,v5) VALUES(?,?,?,?,?,?,?)");


        Failsafe.with(retryPolicy).run(ctx -> {
            if (ctx.isRetry()) {
                retry(ctx);
            }
            conn.setAutoCommit(false);
            int count = 0;
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (List<String> rule : rules) {
                    CasbinRule line = savePolicyLine(ptype, rule);

                    ps.setString(1, line.ptype);
                    ps.setString(2, line.v0);
                    ps.setString(3, line.v1);
                    ps.setString(4, line.v2);
                    ps.setString(5, line.v3);
                    ps.setString(6, line.v4);
                    ps.setString(7, line.v5);
                    ps.addBatch();
                    if (++count == batchSize) {
                        count = 0;
                        ps.executeBatch();
                        ps.clearBatch();
                    }
                }
                if (count != 0) {
                    ps.executeBatch();
                }
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        });
    }

    /**
     * removePolicy removes a policy rule from the storage.
     */
    @Override
    public void removePolicy(String sec, String ptype, List<String> rule) {
        if (CollectionUtils.isEmpty(rule)) {
            return;
        }

        Failsafe.with(retryPolicy).run(ctx -> {
            if (ctx.isRetry()) {
                retry(ctx);
            }
            String sql = renderActualSql("DELETE FROM casbin_rule WHERE ptype = ?");
            int columnIndex = 0;
            for (int i = 0; i < rule.size(); i++) {
                sql = String.format("%s%s%s%s", sql, " AND v", columnIndex, " = ?");
                columnIndex++;
            }
            while (columnIndex <= 5) {
                sql = String.format("%s%s%s%s", sql, " AND v", columnIndex, " IS NULL");
                columnIndex++;
            }
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, ptype);
                for (int j = 0; j < rule.size(); j++) {
                    ps.setString(j + 2, rule.get(j));
                }
                int rows = ps.executeUpdate();
                if (rows < 1 && removePolicyFailed) {
                    throw new CasbinAdapterException(String.format("Remove policy error, remove %d rows, expect least 1 rows", rows));
                }
            }
        });
    }

    @Override
    public void removePolicies(String sec, String ptype, List<List<String>> rules) {
        if (CollectionUtils.isEmpty(rules)) {
            return;
        }

        Failsafe.with(retryPolicy).run(ctx -> {
            if (ctx.isRetry()) {
                retry(ctx);
            }
            conn.setAutoCommit(false);
            try {
                for (List<String> rule : rules) {
                    removePolicy(sec, ptype, rule);
                }
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        });
    }

    /**
     * removeFilteredPolicy removes policy rules that match the filter from the storage.
     */
    @Override
    public void removeFilteredPolicy(String sec, String ptype, int fieldIndex, String... fieldValues) {
        List<String> values = Optional.of(Arrays.asList(fieldValues)).orElse(new ArrayList<>());
        if (CollectionUtils.isEmpty(values)) {
            return;
        }

        Failsafe.with(retryPolicy).run(ctx -> {
            if (ctx.isRetry()) {
                retry(ctx);
            }
            String sql = renderActualSql("DELETE FROM casbin_rule WHERE ptype = ?");
            int columnIndex = fieldIndex;
            for (int i = 0; i < values.size(); i++, columnIndex++) {
                if (Objects.equals(values.get(i), "")) continue;
                sql = String.format("%s%s%s%s", sql, " AND v", columnIndex, " = ?");
            }
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, ptype);
                int index = 2;
                for (String value : values) {
                    if (Objects.equals(value, "")) continue;
                    ps.setString(index++, value);
                }
                int rows = ps.executeUpdate();
                if (rows < 1 && removePolicyFailed) {
                    throw new CasbinAdapterException(String.format("Remove filtered policy error, remove %d rows, expect least 1 rows", rows));
                }
            }
        });
    }

    /**
     * updatePolicy updates a policy rule from the current policy.
     */
    @Override
    public void updatePolicy(String sec, String ptype, List<String> oldRule, List<String> newRule) {
        if (CollectionUtils.isEmpty(oldRule) || CollectionUtils.isEmpty(newRule)) {
            return;
        }

        String sql = renderActualSql("INSERT INTO casbin_rule (ptype,v0,v1,v2,v3,v4,v5) VALUES(?,?,?,?,?,?,?)");


        Failsafe.with(retryPolicy).run(ctx -> {
            if (ctx.isRetry()) {
                retry(ctx);
            }
            conn.setAutoCommit(false);
            removePolicy(sec, ptype, oldRule);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                CasbinRule line = this.savePolicyLine(ptype, newRule);

                ps.setString(1, line.ptype);
                ps.setString(2, line.v0);
                ps.setString(3, line.v1);
                ps.setString(4, line.v2);
                ps.setString(5, line.v3);
                ps.setString(6, line.v4);
                ps.setString(7, line.v5);
                ps.executeUpdate();
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        });
    }

    /**
     * Close the Connection.
     */
    public void close() throws SQLException {
        conn.close();
    }

    protected void retry(ExecutionContext<Void> ctx) throws SQLException {
        if (ctx.getExecutionCount() < _DEFAULT_CONNECTION_TRIES) {
            conn = dataSource.getConnection();
        } else {
            throw new Error(ctx.getLastFailure());
        }
    }

    protected String renderActualSql(String sql) {
        return sql.replace(DEFAULT_TABLE_NAME, tableName);
    }

    private void escapeCasbinRule(CasbinRule line) {
        line.v0 = escapeSingleRule(line.v0);
        line.v1 = escapeSingleRule(line.v1);
        line.v2 = escapeSingleRule(line.v2);
        line.v3 = escapeSingleRule(line.v3);
        line.v4 = escapeSingleRule(line.v4);
        line.v5 = escapeSingleRule(line.v5);
    }

    private String escapeSingleRule(String rule) {
        if (rule.isEmpty() || (rule.startsWith("\"") && rule.endsWith("\""))) {
            return rule;
        }
        return String.format("\"%s\"", rule);
    }
}
