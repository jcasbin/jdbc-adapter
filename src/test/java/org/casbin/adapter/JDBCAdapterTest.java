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

import org.casbin.jcasbin.exception.CasbinAdapterException;
import org.casbin.jcasbin.main.Enforcer;
import org.casbin.jcasbin.persist.file_adapter.FilteredAdapter;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.asList;
import static org.casbin.adapter.JDBCAdapterTestSets.*;
import static org.junit.Assert.fail;

public class JDBCAdapterTest {
    private void testAdapter(List<JDBCAdapter> adapters) {
        for (JDBCAdapter a : adapters) {
            JDBCAdapterTestSets.testAdapter(a);
            JDBCAdapterTestSets.testAddAndRemovePolicy(a);
        }
    }

    @Test
    public void testMySQLAdapter() {
        List<JDBCAdapter> adapters = new ArrayList<>();

        try {
            adapters.add(new MySQLAdapterCreator().create());
            adapters.add(new MySQLAdapterCreator().createViaDataSource());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
            return;
        }

        testAdapter(adapters);

        adapters.forEach(adapter -> {
            try {
                adapter.close();
            } catch (SQLException sqlException) {
                sqlException.printStackTrace();
            }
        });
    }

    @Test
    public void testPgAdapter() {
        List<JDBCAdapter> adapters = new ArrayList<>();

        try {
            adapters.add(new PgAdapterCreator().create());
            adapters.add(new PgAdapterCreator().createViaDataSource());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
            return;
        }

        testAdapter(adapters);

        adapters.forEach(adapter -> {
            try {
                adapter.close();
            } catch (SQLException sqlException) {
                sqlException.printStackTrace();
            }
        });
    }

    @Test
    public void testSQLServerAdapter() {
        List<JDBCAdapter> adapters = new ArrayList<>();

        try {
            adapters.add(new SQLServerAdapterCreator().create());
            adapters.add(new SQLServerAdapterCreator().createViaDataSource());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
            return;
        }

        testAdapter(adapters);

        adapters.forEach(adapter -> {
            try {
                adapter.close();
            } catch (SQLException sqlException) {
                sqlException.printStackTrace();
            }
        });
    }

    @Test
    public void testLoadFilteredPolicyEmptyFilter() throws Exception {
        JDBCAdapter adapter = new MySQLAdapterCreator().create();

        // Because the DB is empty at first,
        // so we need to load the policy from the file adapter (.CSV) first.
        Enforcer e = new Enforcer("examples/rbac_model.conf", "examples/rbac_policy.csv");

        // This is a trick to save the current policy to the DB.
        // We can't call e.savePolicy() because the adapter in the enforcer is still the file adapter.
        // The current policy means the policy in the jCasbin enforcer (aka in memory).
        adapter.savePolicy(e.getModel());

        // Clear the current policy.
        e.clearPolicy();
        testGetPolicy(e, asList());

        // Load the policy from DB.
        adapter.loadFilteredPolicy(e.getModel(), null);
        testGetPolicy(e, asList(
                asList("alice", "data1", "read"),
                asList("bob", "data2", "write"),
                asList("data2_admin", "data2", "read"),
                asList("data2_admin", "data2", "write")));

        // Note: you don't need to look at the above code
        // if you already have a working DB with policy inside.
        e = new Enforcer("examples/rbac_model.conf", adapter);
        testGetPolicy(e, asList(
                asList("alice", "data1", "read"),
                asList("bob", "data2", "write"),
                asList("data2_admin", "data2", "read"),
                asList("data2_admin", "data2", "write")));

        adapter.close();
    }

    @Test
    public void testLoadFilteredPolicyInvalidFilter() throws Exception {
        JDBCAdapter adapter = new MySQLAdapterCreator().create();
        // Because the DB is empty at first,
        // so we need to load the policy from the file adapter (.CSV) first.
        Enforcer e = new Enforcer("examples/rbac_model.conf", "examples/rbac_policy.csv");

        // This is a trick to save the current policy to the DB.
        // We can't call e.savePolicy() because the adapter in the enforcer is still the file adapter.
        // The current policy means the policy in the jCasbin enforcer (aka in memory).
        adapter.savePolicy(e.getModel());

        // Clear the current policy.
        e.clearPolicy();
        testGetPolicy(e, asList());

        // Load the policy from DB.
        Assert.assertThrows(CasbinAdapterException.class, () -> adapter.loadFilteredPolicy(e.getModel(), new Object()));

        adapter.close();
    }

    @Test
    public void testLoadFilteredPolicy() throws Exception {
        JDBCAdapter adapter = new MySQLAdapterCreator().create();

        FilteredAdapter.Filter f = new FilteredAdapter.Filter();
        f.g = new String[]{
                "", "", "domain1"
        };
        f.p = new String[]{
                "", "domain1"
        };

        // Because the DB is empty at first,
        // so we need to load the policy from the file adapter (.CSV) first.
        Enforcer e = new Enforcer("examples/rbac_with_domains_model.conf", "examples/rbac_with_domains_policy.csv");
        // This is a trick to save the current policy to the DB.
        // We can't call e.savePolicy() because the adapter in the enforcer is still the file adapter.
        // The current policy means the policy in the jCasbin enforcer (aka in memory).
        adapter.savePolicy(e.getModel());

        testHasPolicy(e, asList("admin", "domain1", "data1", "read"), true);
        testHasPolicy(e, asList("admin", "domain2", "data2", "read"), true);

        // Clear the current policy.
        e.clearPolicy();
        testGetPolicy(e, asList());

        // Load the policy from DB.
        adapter.loadFilteredPolicy(e.getModel(), f);

        testHasPolicy(e, asList("admin", "domain1", "data1", "read"), true);
        testHasPolicy(e, asList("admin", "domain2", "data2", "read"), false);

        adapter.close();
    }

    @Test
    public void testConstructorParams() throws Exception {
        JDBCAdapter adapter = new MySQLAdapterCreator().create(true, "table_name_test", true);
        JDBCAdapter adapterViaDataSource = new MySQLAdapterCreator().createViaDataSource(true, "table_name_test", true);
        Assert.assertThrows(CasbinAdapterException.class, () -> adapter.removePolicy("p", "p", Arrays.asList("cathy", "data1", "read")));
        Assert.assertThrows(CasbinAdapterException.class, () -> adapterViaDataSource.removePolicy("p", "p", Arrays.asList("cathy", "data1", "read")));
        adapter.close();
        adapterViaDataSource.close();
    }

    @Test
    public void testRemovePolicy() throws Exception {
        JDBCAdapter adapter = new MySQLAdapterCreator().create();

        // Because the DB is empty at first,
        // so we need to load the policy from the file adapter (.CSV) first.
        Enforcer e = new Enforcer("examples/rbac_model.conf", "examples/rbac_policy.csv");

        // This is a trick to save the current policy to the DB.
        // We can't call e.savePolicy() because the adapter in the enforcer is still the file adapter.
        // The current policy means the policy in the jCasbin enforcer (aka in memory).
        adapter.savePolicy(e.getModel());

        e.clearPolicy();
        testGetPolicy(e, asList());

        e = new Enforcer("examples/rbac_model.conf", adapter);
        testGetPolicy(e, asList(
                asList("alice", "data1", "read"),
                asList("bob", "data2", "write"),
                asList("data2_admin", "data2", "read"),
                asList("data2_admin", "data2", "write")));

        adapter.removePolicy("p", "p", Arrays.asList("alice", "data1", "read"));
        e = new Enforcer("examples/rbac_model.conf", adapter);
        testGetPolicy(e, asList(
                asList("bob", "data2", "write"),
                asList("data2_admin", "data2", "read"),
                asList("data2_admin", "data2", "write")));

        adapter.removePolicy("p", "p", Arrays.asList("bob", "data2"));
        e = new Enforcer("examples/rbac_model.conf", adapter);
        testGetPolicy(e, asList(
                asList("bob", "data2", "write"),
                asList("data2_admin", "data2", "read"),
                asList("data2_admin", "data2", "write")));

        adapter.removePolicy("p", "p", Arrays.asList("bob", "data2", "write"));
        e = new Enforcer("examples/rbac_model.conf", adapter);
        testGetPolicy(e, asList(
                asList("data2_admin", "data2", "read"),
                asList("data2_admin", "data2", "write")));
    }
}
