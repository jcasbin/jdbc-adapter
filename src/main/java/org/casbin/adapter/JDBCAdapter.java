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

import org.apache.commons.collections.CollectionUtils;
import org.casbin.jcasbin.model.Assertion;
import org.casbin.jcasbin.model.Model;
import org.casbin.jcasbin.persist.Adapter;
import org.casbin.jcasbin.persist.Helper;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

class CasbinRule {
    String ptype;
    String v0;
    String v1;
    String v2;
    String v3;
    String v4;
    String v5;
}

/**
 * JDBCAdapter is the JDBC adapter for jCasbin.
 * It can load policy from JDBC supported database or save policy to it.
 */
public class JDBCAdapter implements Adapter {
    private String driver;
    private String url;
    private String username;
    private String password;
    private boolean dbSpecified;
    private static final String oracle = "oracle";
    private static final String mysql = "mysql";

    private Connection conn = null;

    /**
     * JDBCAdapter is the constructor for JDBCAdapter.
     *
     * @param driver the JDBC driver, like "com.mysql.cj.jdbc.Driver".
     * @param url the JDBC URL, like "jdbc:mysql://localhost:3306/casbin".
     * @param username the username of the database.
     * @param password the password of the database.
     */
    public JDBCAdapter(String driver, String url, String username, String password) {
        this.driver = driver;
        this.url = url;
        this.username = username;
        this.password = password;
        this.dbSpecified = false;

        open();
    }

    /**
     * JDBCAdapter is the constructor for JDBCAdapter.
     *
     * @param driver the JDBC driver, like "com.mysql.cj.jdbc.Driver".
     * @param url the JDBC URL, like "jdbc:mysql://localhost:3306/casbin".
     * @param username the username of the database.
     * @param password the password of the database.
     * @param dbSpecified whether you have specified an existing DB in url.
     * If dbSpecified == true, you need to make sure the DB in url exists.
     * If dbSpecified == false, the adapter will automatically create a DB named "casbin".
     */
    public JDBCAdapter(String driver, String url, String username, String password, boolean dbSpecified) {
        this.driver = driver;
        this.url = url;
        this.username = username;
        this.password = password;
        this.dbSpecified = dbSpecified;

        open();
    }

    /**
     * The constructor for JDBCAdapter, will not try to create database.
     *
     * @param dataSource the JDBC DataSource.
     */
    public JDBCAdapter(DataSource dataSource) throws SQLException {
    	this.conn = dataSource.getConnection();
        this.url = this.conn.getMetaData().getURL();
        createTable();
    }

    public void finalize() {
        close();
    }

    private void createDatabase() {
        try {
            conn = DriverManager.getConnection(url, username, password);

            Statement stmt = conn.createStatement();
            String sql = "CREATE DATABASE IF NOT EXISTS casbin";
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private String getDataSource(){
        if (url.contains(oracle)){
            return oracle;
        }
        if (url.contains(mysql)){
            return mysql;
        }
        return mysql;
    }

    private String getUrl(){
        if (getDataSource().equals(mysql)){
            return url + "?rewriteBatchedStatements=true&autoReconnect=true";
        }
        return url;
    }

    private void open() {
        try {
            Class.forName(driver);
            if (dbSpecified) {
                conn = DriverManager.getConnection(getUrl(), username, password);
            } else {
                createDatabase();
                conn = DriverManager.getConnection(url + "casbin" + "?rewriteBatchedStatements=true&autoReconnect=true", username, password);
            }

        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }

        createTable();
    }

    private void close() {
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        conn = null;
    }

    private void createTable() {
        try {
            Statement stmt = conn.createStatement();
            String sql = "CREATE TABLE IF NOT EXISTS casbin_rule " +
                    "(ptype VARCHAR(100) not NULL, " +
                    " v0 VARCHAR(100), " +
                    " v1 VARCHAR(100), " +
                    " v2 VARCHAR(100), " +
                    " v3 VARCHAR(100), " +
                    " v4 VARCHAR(100), " +
                    " v5 VARCHAR(100))";
            if (getDataSource().equals(oracle)){
                sql = "declare " +
                        "nCount NUMBER;" +
                        "v_sql LONG;" +
                        "begin " +
                        "SELECT count(*) into nCount FROM USER_TABLES where table_name = 'CASBIN_RULE';" +
                        "IF(nCount <= 0) " +
                        "THEN " +
                        "v_sql:='" +
                        "CREATE TABLE CASBIN_RULE " +
                        "                    (ptype VARCHAR(100) not NULL, " +
                        "                     v0 VARCHAR(100), " +
                        "                     v1 VARCHAR(100), " +
                        "                     v2 VARCHAR(100), " +
                        "                     v3 VARCHAR(100)," +
                        "                     v4 VARCHAR(100)," +
                        "                     v5 VARCHAR(100))';" +
                        "execute immediate v_sql;" +
                        "END IF;" +
                        "end;";
            }
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void dropTable() {
        try {
            Statement stmt = conn.createStatement();
            String sql = "DROP TABLE IF EXISTS casbin_rule";
            if (getDataSource().equals(oracle)){
                sql = "declare " +
                        "nCount NUMBER;" +
                        "v_sql LONG;" +
                        "begin " +
                        "SELECT count(*) into nCount FROM dba_tables where table_name = 'CASBIN_RULE';" +
                        "IF(nCount >= 1) " +
                        "THEN " +
                        "v_sql:='drop table CASBIN_RULE';" +
                        "execute immediate v_sql;" +
                        "END IF;" +
                        "end;";
            }
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void loadPolicyLine(CasbinRule line, Model model) {
        String lineText = line.ptype;
        if (!line.v0.equals("")) {
            lineText += ", " + line.v0;
        }
        if (!line.v1.equals("")) {
            lineText += ", " + line.v1;
        }
        if (!line.v2.equals("")) {
            lineText += ", " + line.v2;
        }
        if (!line.v3.equals("")) {
            lineText += ", " + line.v3;
        }
        if (!line.v4.equals("")) {
            lineText += ", " + line.v4;
        }
        if (!line.v5.equals("")) {
            lineText += ", " + line.v5;
        }

        Helper.loadPolicyLine(lineText, model);
    }

    /**
     * loadPolicy loads all policy rules from the storage.
     */
    @Override
    public void loadPolicy(Model model) {
        try {
            Statement stmt = conn.createStatement();
            ResultSet rSet = stmt.executeQuery("SELECT * FROM casbin_rule");
            ResultSetMetaData rData = rSet.getMetaData();
            while (rSet.next()) {
                CasbinRule line = new CasbinRule();
                for (int i = 1; i <= rData.getColumnCount(); i++) {
                    if (i == 1) {
                        line.ptype = rSet.getObject(i) == null ? "" : (String) rSet.getObject(i);
                    } else if (i == 2) {
                        line.v0 = rSet.getObject(i) == null ? "" : (String) rSet.getObject(i);
                    } else if (i == 3) {
                        line.v1 = rSet.getObject(i) == null ? "" : (String) rSet.getObject(i);
                    } else if (i == 4) {
                        line.v2 = rSet.getObject(i) == null ? "" : (String) rSet.getObject(i);
                    } else if (i == 5) {
                        line.v3 = rSet.getObject(i) == null ? "" : (String) rSet.getObject(i);
                    } else if (i == 6) {
                        line.v4 = rSet.getObject(i) == null ? "" : (String) rSet.getObject(i);
                    } else if (i == 7) {
                        line.v5 = rSet.getObject(i) == null ? "" : (String) rSet.getObject(i);
                    }
                }
                loadPolicyLine(line, model);
            }
            rSet.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
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
        dropTable();
        createTable();

        String sql = "INSERT INTO casbin_rule (ptype,v0,v1,v2,v3,v4,v5) VALUES(?,?,?,?,?,?,?)";
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(sql);


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
            }
        }

        ps.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * addPolicy adds a policy rule to the storage.
     */
    @Override
    public void addPolicy(String sec, String ptype, List<String> rule) {
        if(CollectionUtils.isEmpty(rule)) return;
        String sql = "INSERT INTO casbin_rule (ptype,v0,v1,v2,v3,v4,v5) VALUES(?,?,?,?,?,?,?)";
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(sql);

            CasbinRule line = savePolicyLine(ptype, rule);

            ps.setString(1, line.ptype);
            ps.setString(2, line.v0);
            ps.setString(3, line.v1);
            ps.setString(4, line.v2);
            ps.setString(5, line.v3);
            ps.setString(6, line.v4);
            ps.setString(7, line.v5);
            ps.addBatch();

            ps.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(ps);
        }
    }

    /**
     * removePolicy removes a policy rule from the storage.
     */
    @Override
    public void removePolicy(String sec, String ptype, List<String> rule) {
        if(CollectionUtils.isEmpty(rule)) return;
        removeFilteredPolicy(sec, ptype, 0, rule.toArray(new String[0]));
    }

    /**
     * removeFilteredPolicy removes policy rules that match the filter from the storage.
     */
    @Override
    public void removeFilteredPolicy(String sec, String ptype, int fieldIndex, String... fieldValues) {
        List<String> values = Optional.ofNullable(Arrays.asList(fieldValues)).orElse(new ArrayList<>());
        if(CollectionUtils.isEmpty(values)) return;
        String sql = "DELETE FROM casbin_rule WHERE ptype = ?";
        int columnIndex = fieldIndex;
        for (int i = 0; i < values.size(); i++) {
            sql = String.format("%s%s%s%s", sql, " AND v", columnIndex, " = ?");
            columnIndex++;
        }
        PreparedStatement ps = null;

        try {
            ps = conn.prepareStatement(sql);
            ps.setString(1, ptype);
            for (int j = 0; j < values.size(); j++) {
                ps.setString(j+2, values.get(j));
            }

            ps.addBatch();

            ps.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(ps);
        }
    }

    private void close(PreparedStatement ps) {
        try {
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
