JDBC Adapter
====

[![codebeat badge](https://codebeat.co/badges/df265133-60a0-4ee7-b57d-25fd27273905)](https://codebeat.co/projects/github-com-jcasbin-jdbc-adapter-master)
[![build](https://github.com/jcasbin/jdbc-adapter/actions/workflows/maven-ci.yml/badge.svg)](https://github.com/jcasbin/jdbc-adapter/actions/workflows/maven-ci.yml)
[![codecov](https://codecov.io/gh/jcasbin/jdbc-adapter/branch/master/graph/badge.svg?token=YoXB4Wmvrb)](https://codecov.io/gh/jcasbin/jdbc-adapter)
[![Javadocs](https://www.javadoc.io/badge/org.casbin/jdbc-adapter.svg)](https://www.javadoc.io/doc/org.casbin/jdbc-adapter)
[![Maven Central](https://img.shields.io/maven-central/v/org.casbin/jdbc-adapter.svg)](https://mvnrepository.com/artifact/org.casbin/jdbc-adapter/latest)
[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/casbin/lobby)

JDBC Adapter is the [JDBC](https://en.wikipedia.org/wiki/Java_Database_Connectivity) adapter for [jCasbin](https://github.com/casbin/jcasbin). With this library, jCasbin can load policy from JDBC supported database or save policy to it.

Based on [Supported JDBC Drivers and Databases](https://docs.oracle.com/cd/E19226-01/820-7688/gawms/index.html), The current supported databases are:

- MySQL
- Java DB
- Oracle
- PostgreSQL
- DB2
- Sybase
- Microsoft SQL Server

## Verified Database

- MySQL
- PostgreSQL
- Microsoft SQL Server

We need more developers to help us verify. 

## Installation

For Maven:

```xml
<dependency>
  <groupId>org.casbin</groupId>
  <artifactId>jdbc-adapter</artifactId>
  <version>2.0.1</version>
</dependency>
```

## Simple Example

```java
package com.company.test;

import org.casbin.adapter.JDBCAdapter;
import org.casbin.jcasbin.main.Enforcer;
import com.mysql.cj.jdbc.MysqlDataSource;

public class Test {
    public static void main() {
        String driver = "com.mysql.cj.jdbc.Driver";
        String url = "jdbc:mysql://localhost:3306/db_name";
        String username = "root";
        String password = "123456";

        // The adapter will use the table named "casbin_rule".
        // Use driver, url, username and password to initialize a JDBC adapter.
        JDBCAdapter a = new JDBCAdapter(driver, url, username, password); 
        
        // Recommend use DataSource to initialize a JDBC adapter.
        // Implementer of DataSource interface, such as hikari, c3p0, durid, etc.
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setURL(url);
        dataSource.setUser(username);
        dataSource.setPassword(password);

        a = JDBCAdapter(dataSource);        

        Enforcer e = new Enforcer("examples/rbac_model.conf", a);

        // Check the permission.
        e.enforce("alice", "data1", "read");

        // Modify the policy.
        // e.addPolicy(...);
        // e.removePolicy(...);

        // Save the policy back to DB.
        e.savePolicy();
        // Close the connection.
        a.close();
    }
}
```

## Getting Help

- [jCasbin](https://github.com/casbin/jcasbin)

## License

This project is under Apache 2.0 License. See the [LICENSE](LICENSE) file for the full license text.