/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.api.lineage;

import co.cask.cdap.etl.api.FieldLevelLineage;
import co.cask.cdap.etl.api.TransformStep;
import co.cask.wrangler.api.Step;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Data type for computing lineage for each column in Wrangler
 * Instance of this type to be sent to platform through API
 */
public class WranglerFieldLevelLineage implements FieldLevelLineage {
  private static final String PARSE_KEY = "all columns";

  private enum Type {
    ADD, DROP, MODIFY, READ, RENAME
  }

  public class WranglerBranchingStepNode implements BranchingTransformStepNode {
    final int stepNumber;
    boolean continueUp;
    boolean continueDown;
    Map<String, Integer> upBranches;
    Map<String, Integer> downBranches;

    WranglerBranchingStepNode(int stepNumber, boolean continueUp, boolean continueDown) {
      this.stepNumber = stepNumber;
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
    public int getTransformStepNumber() {
      return this.stepNumber;
    }

    @Override
    public boolean continueBackward() {
      return this.continueDown;
    }

    @Override
    public boolean continueForward() {
      return this.continueUp;
    }

    @Override
    public Map<String, Integer> getImpactedBranches() {
      return Collections.unmodifiableMap(this.upBranches);
    }

    @Override
    public Map<String, Integer> getImpactingBranches() {
      return Collections.unmodifiableMap(this.downBranches);
    }

    @Override
    public String toString() {
      return "(StepNumber: " + stepNumber + ", Continue Up: " + continueUp + ", " + upBranches +
          ", Continue Down: " + continueDown + ", " + downBranches + ")";
    }
  }

  private int currentStepNumber;
  private Map<String, Integer> readColumns;
  private final Set<String> currentColumns;
  private final TransformStep[] steps;
  private final Map<String, List<BranchingTransformStepNode>> lineage;

  public WranglerFieldLevelLineage(int numberOfSteps, List<String> finalColumns) {
    this.currentStepNumber = numberOfSteps;
    this.currentColumns = new HashSet<>(finalColumns.size());
    this.steps = new WranglerTransformStep[numberOfSteps];
    this.lineage = new HashMap<>(finalColumns.size());
    for (String key : finalColumns) {
      this.currentColumns.add(key);
      this.lineage.put(key, new ArrayList<BranchingTransformStepNode>());
    }
  }

  private void create(String key) {
    if (!lineage.containsKey(key)) {
      lineage.put(key, new ArrayList<BranchingTransformStepNode>());
    }
    currentColumns.add(key);
  }

  private List<String> parse(String phrase) {
    String[] commands = phrase.split(" ");
    List<String> list = new ArrayList<>();
    if (commands.length == 2) {
      list.addAll(currentColumns);
    } else if (commands[2].equals("formatted")) {
      String regex = commands[3];
      regex = regex.replaceAll("([\\\\.\\[{}()*+?^$|])", "\\\\$1");
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
  }

  private void insertRead(String key) {
    lineage.get(key).add(new WranglerBranchingStepNode(currentStepNumber, true, true));
    readColumns.put(key, lineage.get(key).size());
  }

  private void insertBranches(String key) {
    List<BranchingTransformStepNode> curr;
    for (String readCol : readColumns.keySet()) {
      curr = lineage.get(readCol);
      ((WranglerBranchingStepNode) curr.get(curr.size() - 1)).putRead(key);
    }
  }

  private void insertModify(String key) {
    WranglerBranchingStepNode node = new WranglerBranchingStepNode(currentStepNumber, true, true);
    insertBranches(key);
    node.downBranches = readColumns;
    lineage.get(key).add(node);
  }

  private void insertDrop(String key) {
    create(key);
    WranglerBranchingStepNode node = new WranglerBranchingStepNode(currentStepNumber, false, true);
    insertBranches(key);
    node.downBranches = readColumns;
    lineage.get(key).add(node);
  }

  private void insertAdd(String key) {
    currentColumns.remove(key);
    WranglerBranchingStepNode node = new WranglerBranchingStepNode(currentStepNumber, true, false);
    insertBranches(key);
    node.downBranches = readColumns;
    lineage.get(key).add(node);
  }

  private void insertRenames(List<String> keys) {
    WranglerBranchingStepNode node;
    BiMap<String, String> renames = HashBiMap.create(keys.size());  // key = old, value = new
    Map<String, WranglerBranchingStepNode> nodes = new HashMap<>(); // new nodes
    for (String key : keys) {
      String[] colSplit = key.split(" ");
      renames.put(colSplit[0], colSplit[1]);
      create(colSplit[0]);
      nodes.put(colSplit[0], null);
      nodes.put(colSplit[1], null);
    }
    for (String key : nodes.keySet()) {
      node = new WranglerBranchingStepNode(currentStepNumber, false, true);
      nodes.put(key, node);
      if (renames.containsKey(key)) {
        node.putRead(renames.get(key));
      } else {
        node.continueUp = true;
        currentColumns.remove(key);
      }
    }
    for (Map.Entry<String, WranglerBranchingStepNode> curr : nodes.entrySet()) {
      lineage.get(curr.getKey()).add(curr.getValue());
    }
    for (String key : renames.values()) {
      node = nodes.get(key);
      node.continueDown = false;
      node.putLineage(renames.inverse().get(key));
    }
  }

  public void store(Step[] parseTree) {
    List<String> columns, renames = new ArrayList<>();

    for (Step currStep : parseTree) {
      Type state = Type.READ;
      String stepName = currStep.getClass().getSimpleName();
      columns = currStep.getColumns();
      renames.clear();
      readColumns = new HashMap<>();
      steps[--currentStepNumber] = new WranglerTransformStep(stepName);

      for (String column : columns) {
        Type curr = Type.valueOf(currStep.getLabel(column).toUpperCase());
        if (curr == Type.READ && state != Type.READ) {
          readColumns = new HashMap<>();
        }
        if (curr != Type.RENAME && state == Type.RENAME) {
          insertRenames(renames);
          renames.clear();
        }
        state = curr;
        switch(curr) {
          case READ:
            if (column.contains(PARSE_KEY)) {
              for (String key : parse(column)) {
                insertRead(key);
              }
            } else {
              insertRead(column);
            }
            break;

          case MODIFY:
            if (column.contains(PARSE_KEY)) {
              for (String key : parse(column)) {
                insertModify(key);
              }
            } else {
              insertModify(column);
            }
            break;

          case ADD:
            if (column.contains(PARSE_KEY)) {
              for (String key : parse(column)) {
                insertAdd(key);
              }
            } else {
              insertAdd(column);
            }
            break;

          case DROP:
            insertDrop(column);
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
        insertRenames(renames);
      }
    }
  }

  @Override
  public List<TransformStep> getSteps() {
    return Collections.unmodifiableList(Arrays.asList(this.steps));
  }

  @Override
  public Map<String, List<BranchingTransformStepNode>> getLineage() {
    return Collections.unmodifiableMap(this.lineage);
  }

  @Override
  public String toString() {
    return "Column Directives: " + lineage;
  }
}
