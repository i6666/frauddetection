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

package spendreport;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

import java.io.IOException;

/**
 * Skeleton code for implementing a fraud detector.
 */
public class FraudDetector2 extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    /**
     * 标记上一笔交易是否为小金额
     */
    private transient ValueState<Boolean> flagState;
    private transient ValueState<Long> timerState;





    @Override
    public void processElement(
            Transaction transaction,
            Context context,
            Collector<Alert> collector) throws Exception {
        /**
         * 使用标记状态来追踪可能的欺诈交易行为。 上一笔交易小于1
         * ValueState 的作用域始终限于当前的 key
         */
        Boolean flag = flagState.value();
        double amount = transaction.getAmount();
        //上一笔没有被标记
        if (!Boolean.TRUE.equals(flag)) {
            //标记下
            if (amount <= SMALL_AMOUNT) {
                flagState.update(true);
                //设置一个计时器，1分钟后 充值下 状态
                long timestamp = context.timerService().currentProcessingTime() + ONE_MINUTE;
                context.timerService().registerProcessingTimeTimer(timestamp);
                timerState.update(timestamp);
            }
        } else {
            //危险
            if (amount >= LARGE_AMOUNT) {
                // 欺诈交易
                Alert alert = new Alert();
                alert.setId(transaction.getAccountId());
                collector.collect(alert);
            }
            //初始化下flag，当标记状态被重置时，删除定时器
            flagState.update(false);
            cleanUp(context);
        }


    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Boolean> stateDescriptor = new ValueStateDescriptor<>("flag", Types.BOOLEAN);
        ValueStateDescriptor<Long> timestampDescriptor = new ValueStateDescriptor<>("timestamp", Types.LONG);
        flagState = getRuntimeContext().getState(stateDescriptor);
        timerState = getRuntimeContext().getState(timestampDescriptor);

    }

    //注册的定时器触发
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
        // remove flag after 1 minute
        timerState.clear();
        flagState.update(false);
    }

    public void cleanUp(Context context) throws IOException {
        context.timerService().deleteEventTimeTimer(timerState.value());
        timerState.clear();
    }
}
