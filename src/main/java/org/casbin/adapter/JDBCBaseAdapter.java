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
import org.casbin.jcasbin.model.Assertion;
import org.casbin.jcasbin.model.Model;
import org.casbin.jcasbin.persist.Adapter;
import org.casbin.jcasbin.persist.BatchAdapter;
import org.casbin.jcasbin.persist.Helper;

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
abstract class JDBCBaseAdapter implements Adapter, BatchAdapter {
    protected static final int _DEFAULT_CONNECTION_TRIES = 3;
    protected DataSource dataSource;
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

    /**
     * The constructor for JDBCAdapter, will not try to create database.
     *
     * @param dataSource the JDBC DataSource.
     */
    protected JDBCBaseAdapter(DataSource dataSource) throws Exception {
        this.dataSource = dataSource;
        migrate();
    }


    protected void migrate() throws SQLException {
        retryPolicy = RetryPolicy.builder()
                .handle(SQLException.class)
                .withDelay(Duration.ofSeconds(1))
                .withMaxRetries(_DEFAULT_CONNECTION_TRIES)
                .build();
        conn = dataSource.getConnection();
        Statement stmt = conn.createStatement();
        String sql = "CREATE TABLE IF NOT EXISTS casbin_rule(id int NOT NULL PRIMARY KEY auto_increment, ptype VARCHAR(100) NOT NULL, v0 VARCHAR(100), v1 VARCHAR(100), v2 VARCHAR(100), v3 VARCHAR(100), v4 VARCHAR(100), v5 VARCHAR(100))";
        String productName = conn.getMetaData().getDatabaseProductName();

        switch (productName) {
            case "MySQL":
                String hasTableSql = "SHOW TABLES LIKE 'casbin_rule';";
                ResultSet rs = stmt.executeQuery(hasTableSql);
                if (rs.next()) {
                    return;
                }
                break;
            case "Oracle":
                sql = "declare begin execute immediate 'CREATE TABLE CASBIN_RULE(id NUMBER(5, 0) not NULL primary key, ptype VARCHAR(100) not NULL, v0 VARCHAR(100), v1 VARCHAR(100), v2 VARCHAR(100), v3 VARCHAR(100), v4 VARCHAR(100), v5 VARCHAR(100))'; " +
                        "exception when others then " +
                        "if SQLCODE = -955 then " +
                            "null; " +
                        "else raise; " +
                        "end if; " +
                        "end;";
                break;
            case "Microsoft SQL Server":
                sql = "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='casbin_rule' and xtype='U') CREATE TABLE casbin_rule(id int NOT NULL primary key identity(1, 1), ptype VARCHAR(100) NOT NULL, v0 VARCHAR(100), v1 VARCHAR(100), v2 VARCHAR(100), v3 VARCHAR(100), v4 VARCHAR(100), v5 VARCHAR(100))";
                break;
            case "PostgreSQL":
                sql = "CREATE SEQUENCE IF NOT EXISTS CASBIN_SEQUENCE START 1;";
                break;
        }

        stmt.executeUpdate(sql);
        if (productName.equals("Oracle")) {
            sql = "declare " +
                    "V_NUM number;" +
                    "BEGIN " +
                    "V_NUM := 0;  " +
                    "select count(0) into V_NUM from user_sequences where sequence_name = 'CASBIN_SEQUENCE';" +
                    "if V_NUM > 0 then " +
                    "null;" +
                    "else " +
                    "execute immediate 'CREATE SEQUENCE casbin_sequence increment by 1 start with 1 nomaxvalue nocycle nocache';" +
                    "end if;END;";
            stmt.executeUpdate(sql);
            sql = "declare " +
                    "V_NUM number;" +
                    "BEGIN " +
                    "V_NUM := 0;" +
                    "select count(0) into V_NUM from user_triggers where trigger_name = 'CASBIN_ID_AUTOINCREMENT';" +
                    "if V_NUM > 0 then " +
                    "null;" +
                    "else " +
                    "execute immediate 'create trigger casbin_id_autoincrement before "+
                    "                        insert on CASBIN_RULE for each row "+
                    "                        when (new.id is null) "+
                    "                        begin "+
                    "                        select casbin_sequence.nextval into:new.id from dual;"+
                    "                        end;';" +
                    "end if;" +
                    "END;";
            stmt.executeUpdate(sql);
        } else if (productName.equals("PostgreSQL")) {
            sql = "CREATE TABLE IF NOT EXISTS casbin_rule(id int NOT NULL PRIMARY KEY default nextval('CASBIN_SEQUENCE'::regclass), ptype VARCHAR(100) NOT NULL, v0 VARCHAR(100), v1 VARCHAR(100), v2 VARCHAR(100), v3 VARCHAR(100), v4 VARCHAR(100), v5 VARCHAR(100))";
            stmt.executeUpdate(sql);
        }
    }

    protected void loadPolicyLine(CasbinRule line, Model model) {
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
                 ResultSet rSet = stmt.executeQuery("SELECT ptype,v0,v1,v2,v3,v4,v5 FROM casbin_rule")) {
                ResultSetMetaData rData = rSet.getMetaData();
                while (rSet.next()) {
                    CasbinRule line = new CasbinRule();
                    line.ptype = rSet.getObject(1) == null ? "" : (String) rSet.getObject(1);
                    line.v0 =  rSet.getObject(2) == null ? "" : (String) rSet.getObject(2);
                    line.v1 =  rSet.getObject(3) == null ? "" : (String) rSet.getObject(3);
                    line.v2 =  rSet.getObject(4) == null ? "" : (String) rSet.getObject(4);
                    line.v3 =  rSet.getObject(5) == null ? "" : (String) rSet.getObject(5);
                    line.v4 =  rSet.getObject(6) == null ? "" : (String) rSet.getObject(6);
                    line.v5 =  rSet.getObject(7) == null ? "" : (String) rSet.getObject(7);
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
        String cleanSql = "delete from casbin_rule";
        String addSql = "INSERT INTO casbin_rule (ptype,v0,v1,v2,v3,v4,v5) VALUES(?,?,?,?,?,?,?)";

        Failsafe.with(retryPolicy).run(ctx -> {
            if (ctx.isRetry()) {
                retry(ctx);
            }
            conn.setAutoCommit(false);

            int count = 0;

            try (Statement statement = conn.createStatement(); PreparedStatement ps = conn.prepareStatement(addSql)) {
                statement.execute(cleanSql);
                for (Map.Entry<String, Assertion> entry : model.model.get("p").entrySet()) {
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
                        if (++count == batchSize) {
                            count = 0;
                            ps.executeBatch();
                            ps.clearBatch();
                        }
                    }
                }

                for (Map.Entry<String, Assertion> entry : model.model.get("g").entrySet()) {
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
                        if (++count == batchSize) {
                            count = 0;
                            ps.executeBatch();
                            ps.clearBatch();
                        }
                    }
                }

                if(count!=0){
                    ps.executeBatch();
                }

                conn.commit();
            } catch (SQLException e) {
                conn.rollback();

                e.printStackTrace();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        });
    }
    /**
     * addPolicy adds a policy rule to the storage.
     */
    @Override
    public void addPolicy(String sec, String ptype, List<String> rule) {
        List<List<String>> rules = new ArrayList<>();
        rules.add(rule);
        this.addPolicies(sec,ptype,rules);
    }

    @Override
    public void addPolicies(String sec, String ptype, List<List<String>> rules) {
        if (CollectionUtils.isEmpty(rules)) {
            return;
        }

        String sql = "INSERT INTO casbin_rule (ptype,v0,v1,v2,v3,v4,v5) VALUES(?,?,?,?,?,?,?)";


        Failsafe.with(retryPolicy).run(ctx -> {
            if (ctx.isRetry()) {
                retry(ctx);
            }
            conn.setAutoCommit(false);
            int count = 0;
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for(List<String> rule:rules){
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
                        count=0;
                        ps.executeBatch();
                        ps.clearBatch();
                    }
                }
                if(count!=0){
                    ps.executeBatch();
                }
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();

                e.printStackTrace();
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
        removeFilteredPolicy(sec, ptype, 0, rule.toArray(new String[0]));
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
                    removeFilteredPolicy(sec, ptype, 0, rule.toArray(new String[0]));
                }
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();

                e.printStackTrace();
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
            String sql = "DELETE FROM casbin_rule WHERE ptype = ?";
            int columnIndex = fieldIndex;
            for (int i = 0; i < values.size(); i++) {
                sql = String.format("%s%s%s%s", sql, " AND v", columnIndex, " = ?");
                columnIndex++;
            }
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, ptype);
                for (int j = 0; j < values.size(); j++) {
                    ps.setString(j + 2, values.get(j));
                }
                ps.executeUpdate();
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
}
