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
import java.util.Collections;
import java.util.Date;
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
    READ, MODIFY, ADD, DROP, RENAME, SWAP
  }

  private enum Action {
    READ, DOWN, STOP, GOTO
  }

  private class ColumnTree {
    BranchingStepNode head;
    BranchingStepNode tail;

    ColumnTree() {
      this.head = new BranchingStepNode();
      this.tail = new BranchingStepNode();
      head.prev = tail.next = null;
      head.next = tail;
      tail.prev = head;
    }

    void add(BranchingStepNode newNode) {
      tail.clone(newNode);
      tail.next = new BranchingStepNode();
      tail.next.prev = tail;
      tail.next.next = null;
      tail = tail.next;
    }

    void addAll(ColumnTree copy) {
      for (BranchingStepNode curr = copy.head.next; !curr.isSentinel(); curr = curr.next) {
        add(curr);
      }
    }

    @Override
    public String toString() {
      StringBuilder returnVal = new StringBuilder("[" + head);
      for (BranchingStepNode curr = head.next; curr != null; curr = curr.next) {
        returnVal.append(", ");
        returnVal.append(curr);
      }
      returnVal.append("]");
      return returnVal.toString();
    }
  }

  private class BranchingStepNode {
    Action action;
    String directive;
    Map<String, BranchingStepNode> branches;
    BranchingStepNode prev, next;

    BranchingStepNode() {  // Sentinel Node
      action = Action.STOP;
      directive = null;
      branches = null;
    }

    BranchingStepNode(String directive, Action action) {
      this.directive = directive;
      this.action = action;
      this.branches = new HashMap<>();
    }

    void putAction(String columnName) {
      branches.put(columnName, lineage.get(columnName).tail.prev);
    }

    void putRead(String columnName) {
      branches.put(columnName, lineage.get(columnName).tail);
    }

    void clone(BranchingStepNode source) {
      this.action = source.action;
      this.directive = source.directive;
      this.branches = source.branches;
    }

    boolean isSentinel() {
      return directive == null;
    }

    private void prettyPrintLineage(String indent, String column, boolean last, boolean start) {
      System.out.print(indent);
      if (last) {
        System.out.print("\\-");
        indent += "  ";
      } else {
        System.out.print("|-");
        indent += "| ";
      }
      BranchingStepNode curr = start ? this : next;
      while (curr.action == Action.READ || curr.action == Action.GOTO) {
        curr = curr.next;
      }
      if (start) {
        System.out.println("Column: " + column);
        if (!isSentinel()) {
          prettyPrintLineage(indent, column, true, false);
        }
      } else {
        System.out.println("Step: " + directive);
        if (action != Action.STOP && !curr.isSentinel()) {
          curr.prettyPrintLineage(indent, column, branches.isEmpty(), false);
        }
        int i = 0;
        for (Map.Entry<String, BranchingStepNode> node : branches.entrySet()) {
          node.getValue().prettyPrintLineage(indent, node.getKey(), i++ == branches.size() - 1, true);
        }
      }
    }

    private void prettyPrintFuture(String indent, String column, boolean last, boolean start) {
      System.out.print(indent);
      if (last) {
        System.out.print("\\-");
        indent += "  ";
      } else {
        System.out.print("|-");
        indent += "| ";
      }
      if (start) {
        System.out.println("Column: " + column);
        if (!isSentinel()) {
          prettyPrintFuture(indent, column, true, false);
        }
      } else {
        System.out.println("Step: " + directive);
        if (action != Action.GOTO && !prev.isSentinel()) {
          prev.prettyPrintFuture(indent, column, branches.isEmpty() || action == Action.DOWN
              || action == Action.STOP, false);
        }
        int i = 0;
        if (action == Action.GOTO || action == Action.READ) {
          for (Map.Entry<String, BranchingStepNode> node : branches.entrySet()) {
            node.getValue().prettyPrintFuture(indent, node.getKey(), i++ == branches.size() - 1, true);
          }
        }
      }
    }

    @Override
    public String toString() {
      if (isSentinel()) {
        return "(Sentinel Node)";
      }
      return "(Step: " + directive + ", Action: " + action.toString().toLowerCase() + ")";
      // + ", Branches: " + branches + ")";
    }
  }

  private final long startTime;
  private final String programName;
  private final Set<String> currentColumns;
  private final Map<String, ColumnTree> lineage;

  public FieldLevelLineage(String[] finalColumns) {
    this.startTime = System.currentTimeMillis();
    this.programName = "wrangler";
    this.currentColumns = new HashSet<>();
    Collections.addAll(currentColumns, finalColumns);
    this.lineage = new HashMap<>(finalColumns.length);
    for (String key : currentColumns) {
      lineage.put(key, new ColumnTree());
    }
  }

  public long getStartTime() {
    return this.startTime;
  }

  public String getProgramName() {
    return this.programName;
  }

  public Map<String, ColumnTree> getLineage() {
    return this.lineage;
  }

  public Set<String> getCurrentColumns() {
    return this.currentColumns;
  }

  private void create(String key) {
    if (!lineage.containsKey(key)) {
      lineage.put(key, new ColumnTree());
    }
    currentColumns.add(key);
  }

  private List<String> parseAndAdd(String phrase) {
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
  }

  private void insertRead(String key, String directive, BranchingStepNode node, BranchingStepNode stopNode) {
    lineage.get(key).add(new BranchingStepNode(directive, Action.READ));
    node.putRead(key);
    stopNode.putRead(key);
  }

  private void insertModify(String key, BranchingStepNode node) {
    for (String readCol : node.branches.keySet()) {
      lineage.get(readCol).tail.prev.putAction(key);
    }
    lineage.get(key).add(node);
  }

  private void insertDrop(String key, BranchingStepNode node) {
    create(key);
    for (String readCol : node.branches.keySet()) {
      lineage.get(readCol).tail.prev.putAction(key);
    }
    lineage.get(key).add(node);
    lineage.get(key).add(new BranchingStepNode(node.directive, Action.GOTO));
  }

  private void insertAdd(String key, BranchingStepNode node) {
    currentColumns.remove(key);
    for (String readCol : node.branches.keySet()) {
      lineage.get(readCol).tail.prev.putAction(key);
    }
    lineage.get(key).add(node);
  }

  private void insertRename(String key, String directive) {
    String[] colSplit = key.split(" ");
    String oldName = colSplit[0];
    String newName = colSplit[1];
    create(oldName);
    currentColumns.remove(newName);
    ColumnTree oldCol = lineage.get(oldName);
    ColumnTree newCol = lineage.get(newName);
    BranchingStepNode oldNode = new BranchingStepNode(directive, Action.GOTO);
    BranchingStepNode newNode = new BranchingStepNode(directive, Action.STOP);
    oldNode.putAction(newName);
    oldCol.add(new BranchingStepNode(directive, Action.STOP));
    oldCol.add(oldNode);
    newCol.add(newNode);
    newNode.putRead(oldName);
  }

  private void insertSwap(String key, String directive) {
    String[] colSplit = key.split(" ");
    String a = colSplit[0];
    String b = colSplit[1];
    ColumnTree colA = lineage.get(a);
    ColumnTree colB = lineage.get(b);
    BranchingStepNode swapNodeA = new BranchingStepNode(directive, Action.STOP);
    BranchingStepNode swapNodeB = new BranchingStepNode(directive, Action.STOP);
    BranchingStepNode readNodeA = new BranchingStepNode(directive, Action.GOTO);
    BranchingStepNode readNodeB = new BranchingStepNode(directive, Action.GOTO);
    readNodeA.putAction(b);
    readNodeB.putAction(a);
    colA.add(swapNodeA);
    colA.add(readNodeA);
    colB.add(swapNodeB);
    colB.add(readNodeB);
    swapNodeA.putRead(b);
    swapNodeB.putRead(a);
  }

  public void store(Step[] steps) {
    boolean readingCols;
    BranchingStepNode node, stopNode;
    List<String> columns;

    for (Step currStep : steps) {
      columns = currStep.getColumns();
      String stepName = currStep.getClass().getSimpleName();
      readingCols = true;
      node = new BranchingStepNode(stepName, Action.DOWN);
      stopNode = new BranchingStepNode(stepName, Action.STOP);

      for (String column : columns) {
        switch(Type.valueOf(currStep.getLabel(column).toUpperCase())) {
          case READ:
            if (!readingCols) {
              node = new BranchingStepNode(stepName, Action.DOWN);
              stopNode = new BranchingStepNode(stepName, Action.STOP);
            }
            if (column.contains("all columns")) {
              for (String key : parseAndAdd(column)) {
                insertRead(key, stepName, node, stopNode);
              }
            } else {
              insertRead(column, stepName, node, stopNode);
            }
            break;

          case MODIFY:
            if (column.contains("all columns")) {
              for (String key : parseAndAdd(column)) {
                insertModify(key, node);
              }
            } else {
              insertModify(column, node);
            }
            readingCols = false;
            break;

          case ADD:
            if (column.contains("all columns")) {
              for (String key : parseAndAdd(column)) {
                insertAdd(key, stopNode);
              }
            } else {
              insertAdd(column, stopNode);
            }
            readingCols = false;
            break;

          case DROP:
            insertDrop(column, node);
            readingCols = false;
            break;

          case RENAME:
            insertRename(column, stepName);
            readingCols = false;
            break;

          case SWAP:
            insertSwap(column, stepName);
            readingCols = false;
            break;

          default:
            System.err.println("Unrecognized label on column: " + column);
            return;
        }
      }
    }
  }

  public void printColumnDirectives(String column, boolean future) {
    if (future) {
      lineage.get(column).tail.prev.prettyPrintFuture("", column, true, true);
    } else {
      lineage.get(column).head.next.prettyPrintLineage("", column, true, true);
    }
  }

  public void append(FieldLevelLineage source) {
    for (Map.Entry<String, ColumnTree> entry : source.lineage.entrySet()) {
      if (!lineage.containsKey(entry.getKey())) {
        lineage.put(entry.getKey(), new ColumnTree());
      }
      lineage.get(entry.getKey()).addAll(entry.getValue());
    }
  }

  @Override
  public String toString() {
    return "Program Name: " + programName + ", Start Time: " + new Date(startTime) + "\n" +
        "Column Directives: " + lineage;
  }
}
