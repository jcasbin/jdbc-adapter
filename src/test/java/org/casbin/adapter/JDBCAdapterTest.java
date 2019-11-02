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

import com.mysql.cj.jdbc.MysqlDataSource;
import org.casbin.jcasbin.main.Enforcer;
import org.casbin.jcasbin.util.Util;
import org.junit.Test;

import javax.sql.DataSource;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class JDBCAdapterTest {
    static void testEnforce(Enforcer e, String sub, Object obj, String act, boolean res) {
        assertEquals(res, e.enforce(sub, obj, act));
    }

    private static void testGetPolicy(Enforcer e, List<List<String>> res) {
        List<List<String>> myRes = e.getPolicy();
        Util.logPrint("Policy: " + myRes);

        if (!Util.array2DEquals(res, myRes)) {
            fail("Policy: " + myRes + ", supposed to be " + res);
        }
    }

    private DataSource genDataSource() {
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setUrl("jdbc:mysql://localhost:3306/casbin");
        dataSource.setDatabaseName("casbin");
        dataSource.setUser("root");
        dataSource.setPassword("");
        return dataSource;
    }

    @Test
    public void testAdapter() {
        // Because the DB is empty at first,
        // so we need to load the policy from the file adapter (.CSV) first.
        Enforcer e = new Enforcer("examples/rbac_model.conf", "examples/rbac_policy.csv");

        JDBCAdapter a = new JDBCAdapter("com.mysql.cj.jdbc.Driver", "jdbc:mysql://localhost:3306/", "root", "");
        // This is a trick to save the current policy to the DB.
        // We can't call e.savePolicy() because the adapter in the enforcer is still the file adapter.
        // The current policy means the policy in the jCasbin enforcer (aka in memory).
        a.savePolicy(e.getModel());

        // Clear the current policy.
        e.clearPolicy();
        testGetPolicy(e, asList());

        // Load the policy from DB.
        a.loadPolicy(e.getModel());
        testGetPolicy(e, asList(
                asList("alice", "data1", "read"),
                asList("bob", "data2", "write"),
                asList("data2_admin", "data2", "read"),
                asList("data2_admin", "data2", "write")));

        // Note: you don't need to look at the above code
        // if you already have a working DB with policy inside.

        // Now the DB has policy, so we can provide a normal use case.
        // Create an adapter and an enforcer.
        // new Enforcer() will load the policy automatically.
        a = new JDBCAdapter(genDataSource());
        e = new Enforcer("examples/rbac_model.conf", a);
        testGetPolicy(e, asList(
                asList("alice", "data1", "read"),
                asList("bob", "data2", "write"),
                asList("data2_admin", "data2", "read"),
                asList("data2_admin", "data2", "write")));
    }

    @Test
    public void testAddAndRemovePolicy() {
        JDBCAdapter a = new JDBCAdapter("com.mysql.cj.jdbc.Driver", "jdbc:mysql://localhost:3306/", "root", "");
        Enforcer e = new Enforcer("examples/rbac_model.conf", a);
        testEnforce(e, "cathy", "data1", "read", false);

        // AutoSave is enabled by default.
        // It can be disabled by:
        // e.enableAutoSave(false);

        // Because AutoSave is enabled, the policy change not only affects the policy in Casbin enforcer,
        // but also affects the policy in the storage.
        e.addPolicy("cathy", "data1", "read");
        testEnforce(e, "cathy", "data1", "read", true);

        // Reload the policy from the storage to see the effect.
        e.clearPolicy();
        a.loadPolicy(e.getModel());
        // The policy has a new rule: {"cathy", "data1", "read"}.
        testEnforce(e, "cathy", "data1", "read", true);

        // Remove the added rule.
        e.removePolicy("cathy", "data1", "read");
        testEnforce(e, "cathy", "data1", "read", false);

        // Reload the policy from the storage to see the effect.
        e.clearPolicy();
        a.loadPolicy(e.getModel());
        testEnforce(e, "cathy", "data1", "read", false);
    }
}
