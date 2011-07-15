/*
 * Copyright 2011 Edmunds.com, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.edmunds.zookeeper.treewatcher;

/**
 * Callback invoked by the tree watcher whenever the current tree is consistent.
 */
public interface ZooKeeperTreeConsistentCallback {
    /**
     * Called when the tree is consistent.
     *
     * @param oldRoot the previous root node from the previous call.
     * @param newRoot the root of the current tree.
     */
    void treeConsistent(ZooKeeperTreeNode oldRoot, ZooKeeperTreeNode newRoot);
}
