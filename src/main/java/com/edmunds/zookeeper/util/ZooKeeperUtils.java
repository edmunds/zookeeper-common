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
package com.edmunds.zookeeper.util;

import org.apache.zookeeper.KeeperException;

import java.util.Arrays;
import java.util.List;

/**
 * ZooKeeper utilities.
 *
 * @author Ryan Holmes
 */
public final class ZooKeeperUtils {

    private ZooKeeperUtils() {
        // This class should never be instantiated.
    }

    private static final List<KeeperException.Code> retryableErrors = Arrays.asList(
        KeeperException.Code.RUNTIMEINCONSISTENCY,
        KeeperException.Code.DATAINCONSISTENCY,
        KeeperException.Code.CONNECTIONLOSS,
        KeeperException.Code.MARSHALLINGERROR,
        KeeperException.Code.OPERATIONTIMEOUT);

    public static boolean isRetryableError(KeeperException.Code code) {
        return retryableErrors.contains(code);
    }
}
