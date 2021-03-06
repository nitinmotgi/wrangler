/*
 *  Copyright © 2017 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package co.cask.wrangler.test.api;

import co.cask.wrangler.api.Row;
import edu.emory.mathcs.backport.java.util.Collections;

import java.util.ArrayList;
import java.util.List;

/**
 * Class description here.
 */
public class TestRows {
  private final List<Row> rows;

  public TestRows() {
    this.rows = new ArrayList<>();
  }

  public void add(Row row) {
    rows.add(row);
  }

  public List<Row> toList() {
    Collections.reverse(rows);
    return rows;
  }
}
