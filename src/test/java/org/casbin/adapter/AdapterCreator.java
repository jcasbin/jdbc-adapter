// Copyright 2020 The casbin Authors. All Rights Reserved.
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

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.mysql.cj.jdbc.MysqlDataSource;
import oracle.jdbc.pool.OracleDataSource;
import org.postgresql.ds.PGSimpleDataSource;

public interface AdapterCreator {
    JDBCAdapter create() throws Exception;

    JDBCAdapter createViaDataSource() throws Exception;
}


class MySQLAdapterCreator implements AdapterCreator {
    private String url = "jdbc:mysql://localhost:3306/casbin?serverTimezone=GMT%2B8";
    private String username = "root";
    private String password = "yourPasswordHere";
    private String driver = "com.mysql.cj.jdbc.Driver";

    @Override
    public JDBCAdapter create() throws Exception {
        return new JDBCAdapter(driver, url, username, password);
    }

    @Override
    public JDBCAdapter createViaDataSource() throws Exception {
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setURL(url);
        dataSource.setUser(username);
        dataSource.setPassword(password);

        return new JDBCAdapter(dataSource);
    }
}


class OracleAdapterCreator implements AdapterCreator {
    private String url = "jdbc:oracle:thin:@//localhost:1521/orcl";
    private String username = "yourUsername";
    private String password = "yourPasswordHere";
    private String driver = "oracle.jdbc.driver.OracleDriver";

    @Override
    public JDBCAdapter create() throws Exception {
        return new JDBCAdapter(driver, url, username, password);
    }

    @Override
    public JDBCAdapter createViaDataSource() throws Exception {
        OracleDataSource dataSource = new OracleDataSource();
        dataSource.setURL(url);
        dataSource.setUser(username);
        dataSource.setPassword(password);

        return new JDBCAdapter(dataSource);
    }
}

class PgAdapterCreator implements AdapterCreator {
    private String url = "jdbc:postgresql://localhost:5432/casbin";
    private String username = "postgres";
    private String password = "";
    private String driver = "org.postgresql.Driver";

    @Override
    public JDBCAdapter create() throws Exception {
        return new JDBCAdapter(driver, url, username, password);
    }

    @Override
    public JDBCAdapter createViaDataSource() throws Exception {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setURL(url);
        dataSource.setUser(username);
        dataSource.setPassword(password);

        return new JDBCAdapter(dataSource);
    }
}

class SQLServerAdapterCreator implements AdapterCreator {
    private String url = "jdbc:sqlserver://localhost:1433;databaseName=casbin;";
    private String username = "sa";
    private String password = "9G3iqmzQDw9zCXII";
    private String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

    @Override
    public JDBCAdapter create() throws Exception {
        return new JDBCAdapter(driver, url, username, password);
    }

    @Override
    public JDBCAdapter createViaDataSource() throws Exception {
        SQLServerDataSource dataSource = new SQLServerDataSource();
        dataSource.setURL(url);
        dataSource.setUser(username);
        dataSource.setPassword(password);

        return new JDBCAdapter(dataSource);
    }
}
