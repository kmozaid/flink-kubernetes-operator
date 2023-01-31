/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package autoscaling;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/** Autoscaling Example. */
public class AutoscalingExample {

    private static final Logger LOG = LoggerFactory.getLogger(AutoscalingExample.class);

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        var numberOfPipelines = args.length == 1 ? Long.parseLong(args[0]) : 20;
        for (int i = 0; i < numberOfPipelines; i++) {
            String pipelineName = "autoscaling-" + i;
            LOG.info("Running autoscaling {}", pipelineName);
            env.fromSequence(0, 60)
                    .uid("source-" + i)
                    .shuffle()
                    .map(
                            num -> {
                                sleep();
                                return num;
                            })
                    .print()
                    .uid("sink-" + i);
            env.execute(pipelineName);
            LOG.info("Completed pipeline {}", pipelineName);
            Thread.sleep(5000);
        }
    }

    private static void sleep() {
        try {
            Thread.sleep(250);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
