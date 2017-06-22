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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * DataType for storing directives for each column
 * Instance of this type to be sent to platform through API
 */

public class FieldLevelLineage {
  private enum Type {
    READ("read"),
    MODIFY("modify"),
    ADD("add"),
    DROP("drop"),
    RENAME("rename"),
    SWAP("swap");

    private final String type;

    Type(final String type) {
      this.type = type;
    }

    @Override
    public String toString() {
      return type;
    }
  }

  private class BranchingStepNode {
    boolean continueDown;
    Step directive;
    Map<String, Integer> branches;

    BranchingStepNode(Step directive, boolean continueDown) {
      this.directive = directive;
      this.branches = new HashMap<>();
      this.continueDown = continueDown;
    }

    void put(String columnName, int skips) {
      branches.put(columnName, skips);
    }

    @Override
    public String toString() {
      return "(Step: " + directive.getClass().getSimpleName() + ", Branches: " + branches + ")";
    }
  }

  private final long startTime;
  private final String programName;
  private final String dataSetName; // dataSet/stream name/id
  private final String[] finalColumns;
  private final Set<String> currentColumns; // not sure if needed
  private final Map<String, List<BranchingStepNode>> lineage;

  public FieldLevelLineage(String dataSetName, String[] columnNames) {
    this.startTime = System.currentTimeMillis();
    this.programName = "wrangler";
    this.dataSetName = dataSetName;
    this.finalColumns = columnNames.clone();
    this.lineage = new HashMap<>(columnNames.length);
    this.currentColumns = new HashSet<>(columnNames.length);
    for(String key : columnNames) {
      lineage.put(key, new ArrayList<BranchingStepNode>());
      currentColumns.add(key);
    }
  }

  public long getStartTime() {
    return this.startTime;
  }

  public String getProgramName() {
    return this.programName;
  }

  public String getDataSetName() {
    return this.dataSetName;
  }

  public String[] getFinalColumns() {
    return this.finalColumns;
  }

  private void create(String key) {
    if(!lineage.containsKey(key)) {
      lineage.put(key, new ArrayList<BranchingStepNode>());
    }
    currentColumns.add(key);
  }

  public void store(Step[] steps) {
    BranchingStepNode node, treeNode;
    String[] colSplit;
    List<String> columns, readCols, addCols, modifyCols, dropCols, renameCols, swapCols;

    for(Step currStep : steps) {
      columns = currStep.getColumns();
      readCols = new ArrayList<>();
      addCols = new ArrayList<>();
      modifyCols = new ArrayList<>();
      dropCols = new ArrayList<>();
      renameCols = new ArrayList<>();
      swapCols = new ArrayList<>();

      for(String column : columns) {
        switch(Type.valueOf(column.getType())) {
          case READ:
            readCols.add(column);
            break;

          case MODIFY:
            modifyCols.add(column);
            break;

          case ADD:
            addCols.add(column);
            break;

          case DROP:
            dropCols.add(column);
            break;

          case RENAME:
            renameCols.add(column);
            break;

          case SWAP:
            swapCols.add(column);
            break;
        }
      }

      node = new BranchingStepNode(currStep, true);
      for(String key : renameCols) {
        colSplit = key.split(" ");
        String oldName = colSplit[0];
        String newName = colSplit[1];
        create(oldName);
        currentColumns.remove(newName);
        lineage.get(oldName).add(node);
        treeNode = new BranchingStepNode(currStep, false);
        treeNode.put(oldName, lineage.get(oldName).size());
        lineage.get(newName).add(treeNode);
      }

      for(String key : swapCols) {
        colSplit = key.split(" ");
        String a = colSplit[0];
        String b = colSplit[1];
        treeNode = new BranchingStepNode(currStep, false);
        treeNode.put(b, lineage.get(b).size() + 1);
        lineage.get(a).add(treeNode);
        treeNode = new BranchingStepNode(currStep, false);
        treeNode.put(a, lineage.get(a).size());
        lineage.get(b).add(treeNode);
      }

      node = new BranchingStepNode(currStep, true);
      treeNode = new BranchingStepNode(currStep, false);
      for(String key : readCols) {
        node.put(key, lineage.get(key).size());
        treeNode.put(key, lineage.get(key).size());
      }

      for(String key : modifyCols) {
        lineage.get(key).add(node);
      }

      for(String key : dropCols) {
        create(key);
        lineage.get(key).add(node);
      }

      for(String key : addCols) {
        currentColumns.remove(key);
        lineage.get(key).add(treeNode);
      }
    }
  }

  @Override
  public String toString() {
    return "Workspace Name: " + dataSetName + ", Program Name: " + programName + ", Start Time: " + startTime +
        "\n" + "Column Directives: " + lineage;
  }
}
