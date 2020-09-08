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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

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
    }

    @Test
    public void testOracleAdapter() {
        List<JDBCAdapter> adapters = new ArrayList<>();

        try {
            adapters.add(new OracleAdapterCreator().create());
            adapters.add(new OracleAdapterCreator().createViaDataSource());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
            return;
        }

        testAdapter(adapters);
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
    }
}
