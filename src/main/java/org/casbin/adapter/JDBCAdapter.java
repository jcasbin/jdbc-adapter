// Copyright 2021 The casbin Authors. All Rights Reserved.
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

import dev.failsafe.Failsafe;
import org.casbin.jcasbin.exception.CasbinAdapterException;
import org.casbin.jcasbin.model.Model;
import org.casbin.jcasbin.persist.FilteredAdapter;
import org.casbin.jcasbin.persist.Helper;
import org.casbin.jcasbin.persist.file_adapter.FilteredAdapter.Filter;

import javax.sql.DataSource;
import java.sql.*;

/**
 * JDBCFilteredAdapter is the JDBC adapter for jCasbin.
 * JDBCFilteredAdapter can load policy from JDBC supported database or save policy to it and it supports filtered policies .
 *
 * @author shy
 * @since 2021/01/26
 */
public class JDBCAdapter extends JDBCBaseAdapter implements FilteredAdapter {

    private boolean isFiltered = false;

    /**
     * JDBCAdapter is the constructor for JDBCAdapter.
     *
     * @param driver   the JDBC driver, like "com.mysql.cj.jdbc.Driver".
     * @param url      the JDBC URL, like "jdbc:mysql://localhost:3306/casbin".
     * @param username the username of the database.
     * @param password the password of the database.
     */
    public JDBCAdapter(String driver, String url, String username, String password) throws Exception {
        super(driver, url, username, password);
    }

    /**
     * The constructor for JDBCAdapter, will not try to create database.
     *
     * @param dataSource the JDBC DataSource.
     */
    public JDBCAdapter(DataSource dataSource) throws Exception {
        super(dataSource);
    }

    /**
     * JDBCAdapter is the constructor for JDBCAdapter.
     *
     * @param driver             the JDBC driver, like "com.mysql.cj.jdbc.Driver".
     * @param url                the JDBC URL, like "jdbc:mysql://localhost:3306/casbin".
     * @param username           the username of the database.
     * @param password           the password of the database.
     * @param removePolicyFailed whether to throw an exception when delete strategy fails.
     * @param tableName          the table name of casbin rule.
     * @param autoCreateTable    whether to create the table automatically.
     */
    public JDBCAdapter(String driver, String url, String username, String password, boolean removePolicyFailed, String tableName, boolean autoCreateTable) throws Exception {
        super(driver, url, username, password, removePolicyFailed, tableName, autoCreateTable);
    }

    /**
     * JDBCAdapter is the constructor for JDBCAdapter.
     *
     * @param dataSource         the JDBC DataSource.
     * @param removePolicyFailed whether to throw an exception when delete strategy fails.
     * @param tableName          the table name of casbin rule.
     * @param autoCreateTable    whether to create the table automatically.
     */
    public JDBCAdapter(DataSource dataSource, boolean removePolicyFailed, String tableName, boolean autoCreateTable) throws Exception {
        super(dataSource, removePolicyFailed, tableName, autoCreateTable);
    }

    /**
     * loadFilteredPolicy loads only policy rules that match the filter.
     *
     * @param model  the model.
     * @param filter the filter used to specify which type of policy should be loaded.
     * @throws CasbinAdapterException if the file path or the type of the filter is incorrect.
     */
    @Override
    public void loadFilteredPolicy(Model model, Object filter) throws CasbinAdapterException {
        if (filter == null) {
            loadPolicy(model);
            isFiltered = false;
            return;
        }
        if (!(filter instanceof Filter)) {
            isFiltered = false;
            throw new CasbinAdapterException("Invalid filter type.");
        }
        try {
            loadFilteredPolicyFile(model, (Filter) filter, Helper::loadPolicyLine);
            isFiltered = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * IsFiltered returns true if the loaded policy has been filtered.
     *
     * @return true if have any filter roles.
     */
    @Override
    public boolean isFiltered() {
        return isFiltered;
    }

    /**
     * loadFilteredPolicyFile loads only policy rules that match the filter from file.
     */
    private void loadFilteredPolicyFile(Model model, Filter filter, Helper.loadPolicyLineHandler<String, Model> handler) throws CasbinAdapterException {
        Failsafe.with(retryPolicy).run(ctx -> {
            if (ctx.isRetry()) {
                retry(ctx);
            }
            try (Statement stmt = conn.createStatement();
                 ResultSet rSet = stmt.executeQuery(renderActualSql("SELECT * FROM casbin_rule"))) {
                ResultSetMetaData rData = rSet.getMetaData();
                while (rSet.next()) {
                    CasbinRule line = new CasbinRule();
                    for (int i = 1; i <= rData.getColumnCount(); i++) {
                        if (i == 2) {
                            line.ptype = rSet.getObject(i) == null ? "" : (String) rSet.getObject(i);
                        } else if (i == 3) {
                            line.v0 = rSet.getObject(i) == null ? "" : (String) rSet.getObject(i);
                        } else if (i == 4) {
                            line.v1 = rSet.getObject(i) == null ? "" : (String) rSet.getObject(i);
                        } else if (i == 5) {
                            line.v2 = rSet.getObject(i) == null ? "" : (String) rSet.getObject(i);
                        } else if (i == 6) {
                            line.v3 = rSet.getObject(i) == null ? "" : (String) rSet.getObject(i);
                        } else if (i == 7) {
                            line.v4 = rSet.getObject(i) == null ? "" : (String) rSet.getObject(i);
                        } else if (i == 8) {
                            line.v5 = rSet.getObject(i) == null ? "" : (String) rSet.getObject(i);
                        }
                    }
                    if (filterLine(line, filter)) {
                        continue;
                    }
                    loadPolicyLine(line, model);
                }
            }
        });
    }

    /**
     * match the line.
     */
    private boolean filterLine(CasbinRule line, Filter filter) {
        if (filter == null) {
            return false;
        }
        String[] filterSlice = null;
        switch (line.ptype.trim()) {
            case "p":
                filterSlice = filter.p;
                break;
            case "g":
                filterSlice = filter.g;
                break;
        }
        if (filterSlice == null) {
            filterSlice = new String[]{};
        }
        return filterWords(line.toStringArray(), filterSlice);
    }

    /**
     * match the words in the specific line.
     */
    private boolean filterWords(String[] line, String[] filter) {
        boolean skipLine = false;
        int i = 0;
        for (String s : filter) {
            i++;
            if (s.length() > 0 && !s.trim().equals(line[i].trim())) {
                skipLine = true;
                break;
            }
        }
        return skipLine;
    }
}
