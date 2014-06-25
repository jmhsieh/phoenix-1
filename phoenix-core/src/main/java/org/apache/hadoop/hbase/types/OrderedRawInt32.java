/**
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
package org.apache.hadoop.hbase.types;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.OrderedBytes;
import org.apache.hadoop.hbase.util.PositionedByteRange;


/**
 * An {@code int} of 32-bits using a fixed-length encoding. Built on
 * {@link OrderedBytes#encodeInt32(PositionedByteRange, int, Order)}.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class OrderedRawInt32 extends OrderedBytesBase<Integer> {

    public static final OrderedRawInt32 ASCENDING = new OrderedRawInt32(Order.ASCENDING);
    public static final OrderedRawInt32 DESCENDING = new OrderedRawInt32(Order.DESCENDING);

    protected OrderedRawInt32(Order order) { super(order); }

    @Override
    public boolean isNullable() { return false; }

    @Override
    public int encodedLength(Integer val) { return 4; }

    @Override
    public Class<Integer> encodedClass() { return Integer.class; }

    @Override
    public Integer decode(PositionedByteRange src) {
        int val = (order.apply(src.get()) ^ 0x80) & 0xff;
        for (int i = 1; i < 4; i++) {
            val = (val << 8) + (order.apply(src.get()) & 0xff);
        }
        return val;
    }

    @Override
    public int encode(PositionedByteRange dst, Integer val) {
        if (null == val) throw new IllegalArgumentException("Null values not supported.");
        // if no destination, just return size.
        if (dst == null) return 4;

        final int offset = dst.getOffset(), start = dst.getPosition();
        dst     .put((byte) ((val >> 24) ^ 0x80))
                .put((byte) (val >> 16))
                .put((byte) (val >> 8))
                .put((byte) (int) val);
        order.apply(dst.getBytes(), offset + start, 4);
        return 4;
    }

    /**
     * Read an {@code int} value from the buffer {@code src}.
     */
    public int decodeInt(PositionedByteRange src) {
        int val = (order.apply(src.get()) ^ 0x80) & 0xff;
        for (int i = 1; i < 4; i++) {
            val = (val << 8) + (order.apply(src.get()) & 0xff);
        }
        return val;
    }

    /**
     * Write instance {@code val} into buffer {@code dst}.
     */
    public int encodeInt(PositionedByteRange dst, int val) {
        final int offset = dst.getOffset(), start = dst.getPosition();
        dst     .put((byte) ((val >> 24) ^ 0x80))
                .put((byte) (val >> 16))
                .put((byte) (val >> 8))
                .put((byte) (int) val);
        order.apply(dst.getBytes(), offset + start, 4);
        return 4;
    }
}