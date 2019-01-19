JDBC Adapter
====

[![codebeat badge](https://codebeat.co/badges/df265133-60a0-4ee7-b57d-25fd27273905)](https://codebeat.co/projects/github-com-jcasbin-jdbc-adapter-master)
[![Build Status](https://travis-ci.org/jcasbin/jdbc-adapter.svg?branch=master)](https://travis-ci.org/jcasbin/jdbc-adapter)
[![Coverage Status](https://coveralls.io/repos/github/jcasbin/jdbc-adapter/badge.svg?branch=master)](https://coveralls.io/github/jcasbin/jdbc-adapter?branch=master)
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

## Installation

For Maven:

```xml
<dependency>
  <groupId>org.casbin</groupId>
  <artifactId>jdbc-adapter</artifactId>
  <version>1.1.2-FIX</version>
</dependency>
```

## Simple Example

```java
package com.company.test;

import org.casbin.jcasbin.main.Enforcer;
import org.casbin.jcasbin.util.Util;
import org.casbin.adapter.JDBCAdapter;

public class Test {
    public static void main() {
        // Initialize a JDBC adapter and use it in a jCasbin enforcer:
        // The adapter will use the MySQL database named "casbin".
        // If it doesn't exist, the adapter will create it automatically.
        JDBCAdapter a = new JDBCAdapter("com.mysql.cj.jdbc.Driver", "jdbc:mysql://localhost:3306/", "root", "123"); // Your driver and URL. 

        // Or you can use an existing DB "abc" like this:
        // The adapter will use the table named "casbin_rule".
        // If it doesn't exist, the adapter will create it automatically.
        // JDBCAdapter a = new JDBCAdapter("com.mysql.cj.jdbc.Driver", "jdbc:mysql://localhost:3306/casbin", "root", "123", true);

        Enforcer e = new Enforcer("examples/rbac_model.conf", a);

        // Load the policy from DB.
        e.loadPolicy();

        // Check the permission.
        e.enforce("alice", "data1", "read");

        // Modify the policy.
        // e.addPolicy(...);
        // e.removePolicy(...);

        // Save the policy back to DB.
        e.savePolicy();
    }
}
```

## Getting Help

- [jCasbin](https://github.com/casbin/jcasbin)

## License

This project is under Apache 2.0 License. See the [LICENSE](LICENSE) file for the full license text.