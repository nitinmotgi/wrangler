/*
 * Copyright © 2016-2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.wrangler.executor;

import co.cask.wrangler.api.Step;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * DataType for computing lineage for each column
 * Instance of this type to be sent to platform through API
 * Sent as String with help of Gson
 */

public class FieldLevelLineage {
  private enum Type {
    ADD, DROP, MODIFY, READ, RENAME
  }

  class BranchingStepNode {
    boolean continueUp, continueDown;
    String directive;
    Map<String, Integer> upBranches, downBranches;

    BranchingStepNode(String directive, boolean continueUp, boolean continueDown) {
      this.directive = directive;
      this.continueUp = continueUp;
      this.continueDown = continueDown;
      this.upBranches = new HashMap<>();
      this.downBranches = new HashMap<>();
    }

    void putRead(String columnName) {
      upBranches.put(columnName, lineage.get(columnName).size() - 1);
    }

    void putLineage(String columnName) {
      downBranches.put(columnName, lineage.get(columnName).size());
    }

    @Override
    public String toString() {
      return "(Step: " + directive + ", Continue Up: " + continueUp + ", " + upBranches +
          ", Continue Down: " + continueDown + ", " + downBranches + ")";
    }
  }

  private static final String PROGRAM = "wrangler";
  private static final String PARSE_KEY = "all columns";
  private final long startTime;
  private Set<String> startColumns;
  private final Set<String> currentColumns, endColumns;
  private final Map<String, List<BranchingStepNode>> lineage;

  public FieldLevelLineage(String[] finalColumns) {
    this.startTime = System.currentTimeMillis();
    this.startColumns = new HashSet<>();
    this.currentColumns = new HashSet<>(finalColumns.length);
    this.endColumns = new HashSet<>(finalColumns.length);
    this.lineage = new HashMap<>(finalColumns.length);
    for (String key : finalColumns) {
      this.currentColumns.add(key);
      this.endColumns.add(key);
      this.lineage.put(key, new ArrayList<BranchingStepNode>());
    }
  }

  public long getStartTime() {
    return this.startTime;
  }

  public static String getProgramName() {
    return PROGRAM;
  }

  public Set<String> getCurrentColumns() {
    return this.currentColumns;
  }

  public Set<String> getStartColumns() {
    return this.startColumns;
  }

  public Set<String> getEndColumns() {
    return this.endColumns;
  }

  public Map<String, List<BranchingStepNode>> getLineage() {
    return this.lineage;
  }

  private void create(String key) {
    if (!lineage.containsKey(key)) {
      lineage.put(key, new ArrayList<BranchingStepNode>());
    }
    currentColumns.add(key);
  }

  private List<String> parse(String phrase) {
    try {
      String[] commands = phrase.split(" ");
      List<String> list = new ArrayList<>();
      if (commands.length == 2) {
        list.addAll(currentColumns);
      } else if (commands[2].equals("formatted")) {
        String regex = commands[3];
        regex = regex.replaceAll("([\\\\.\\[{(*+?^$|])", "\\\\$1");
        regex = regex.replaceAll("%s", ".+");
        regex = regex.replaceAll("%d", "[0-9]+");
        for (String key : currentColumns) {
          if (key.matches(regex)) {
            list.add(key);
          }
        }
      } else {
        list.addAll(currentColumns);
        for (int i = 3; i < commands.length; ++i) {
          list.remove(commands[i]);
        }
      }
      return list;
    } catch (Exception e) {
      System.err.println("Improper format");
      return new ArrayList<>();
    }
  }

  private void insertRead(String key, String directive, Map<String, Integer> readCols) {
    lineage.get(key).add(new BranchingStepNode(directive, true, true));
    readCols.put(key, lineage.get(key).size());
  }

  private void insertBranches(String key, Set<String> readCols) {
    List<BranchingStepNode> curr;
    for (String readCol : readCols) {
      curr = lineage.get(readCol);
      curr.get(curr.size() - 1).putRead(key);
    }
  }

  private void insertModify(String key, String directive, Map<String, Integer> readCols) {
    BranchingStepNode node = new BranchingStepNode(directive, true, true);
    insertBranches(key, readCols.keySet());
    node.downBranches = readCols;
    lineage.get(key).add(node);
  }

  private void insertDrop(String key, String directive, Map<String, Integer> readCols) {
    create(key);
    BranchingStepNode node = new BranchingStepNode(directive, false, true);
    insertBranches(key, readCols.keySet());
    node.downBranches = readCols;
    lineage.get(key).add(node);
  }

  private void insertAdd(String key, String directive, Map<String, Integer> readCols) {
    currentColumns.remove(key);
    BranchingStepNode node = new BranchingStepNode(directive, true, false);
    insertBranches(key, readCols.keySet());
    node.downBranches = readCols;
    lineage.get(key).add(node);
  }

  private void insertRenames(List<String> keys, String directive) {
    BranchingStepNode node;
    BiMap<String, String> renames = HashBiMap.create();  // key = old, value = new
    Map<String, BranchingStepNode> nodes = new HashMap<>(); // new nodes
    for (String key : keys) {
      String[] colSplit = key.split(" ");
      renames.put(colSplit[0], colSplit[1]);
      create(colSplit[0]);
      nodes.put(colSplit[0], null);
      nodes.put(colSplit[1], null);
    }
    for (String key : nodes.keySet()) {
      node = new BranchingStepNode(directive, false, true);
      nodes.put(key, node);
      if (renames.containsKey(key)) {
        node.putRead(renames.get(key));
      } else {
        node.continueUp = true;
        currentColumns.remove(key);
      }
    }
    for (Map.Entry<String, BranchingStepNode> curr : nodes.entrySet()) {
      lineage.get(curr.getKey()).add(curr.getValue());
    }
    for (String key : renames.values()) {
      node = nodes.get(key);
      node.continueDown = false;
      node.putLineage(renames.inverse().get(key));
    }
  }

  public void store(Step[] steps) {
    Type curr, state;
    String stepName;
    List<String> columns, renames;
    Map<String, Integer> reads;

    for (Step currStep : steps) {
      state = Type.READ;
      stepName = currStep.getClass().getSimpleName();
      columns = currStep.getColumns();
      renames = new ArrayList<>();
      reads = new HashMap<>();

      for (String column : columns) {
        curr = Type.valueOf(currStep.getLabel(column).toUpperCase());
        if (curr == Type.READ && state != Type.READ) {
          reads = new HashMap<>();
        }
        if (curr != Type.RENAME && state == Type.RENAME) {
          insertRenames(renames, stepName);
          renames = new ArrayList<>();
        }
        state = curr;
        switch(curr) {
          case READ:
            if (column.contains(PARSE_KEY)) {
              for (String key : parse(column)) {
                insertRead(key, stepName, reads);
              }
            } else {
              insertRead(column, stepName, reads);
            }
            break;

          case MODIFY:
            if (column.contains(PARSE_KEY)) {
              for (String key : parse(column)) {
                insertModify(key, stepName, reads);
              }
            } else {
              insertModify(column, stepName, reads);
            }
            break;

          case ADD:
            if (column.contains(PARSE_KEY)) {
              for (String key : parse(column)) {
                insertAdd(key, stepName, reads);
              }
            } else {
              insertAdd(column, stepName, reads);
            }
            break;

          case DROP:
            insertDrop(column, stepName, reads);
            break;

          case RENAME:
            renames.add(column);
            break;

          default:
            System.err.println("Unrecognized label on column: " + column);
            return;
        }
      }
      if (state == Type.RENAME) {
        insertRenames(renames, stepName);
      }
    }
    startColumns.addAll(currentColumns);
  }

  @Override
  public String toString() {
    return "Program Name: " + PROGRAM + ", Start Time: " + new Date(startTime) + "\n" +
        "Column Directives: " + lineage;
  }
}
