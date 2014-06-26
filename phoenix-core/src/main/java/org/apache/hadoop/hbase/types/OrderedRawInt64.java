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
import org.apache.hadoop.hbase.util.PositionedByteRange;


/**
 * An {@code int} of 64-bits using a fixed-length encoding. Built on
 * {@link org.apache.hadoop.hbase.util.OrderedBytes#encodeInt64(org.apache.hadoop.hbase.util.PositionedByteRange, int, org.apache.hadoop.hbase.util.Order)}.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class OrderedRawInt64 extends OrderedBytesBase<Long> {

    public static final OrderedRawInt64 ASCENDING = new OrderedRawInt64(Order.ASCENDING);
    public static final OrderedRawInt64 DESCENDING = new OrderedRawInt64(Order.DESCENDING);

    protected OrderedRawInt64(Order order) { super(order); }

    @Override
    public boolean isNullable() { return false; }

    @Override
    public int encodedLength(Long val) { return 8; }

    @Override
    public Class<Long> encodedClass() { return Long.class; }

    @Override
    public Long decode(PositionedByteRange src) {
        long val = (order.apply(src.get()) ^ 0x80) & 0xff;
        for (int i = 1; i < 8; i++) {
            val = (val << 8) + (order.apply(src.get()) & 0xff);
        }
        return val;
    }

    @Override
    public int encode(PositionedByteRange dst, Long val) {
        if (null == val) throw new IllegalArgumentException("Null values not supported.");
        // if no destination, just return size.
        if (dst == null) return 8;

        final int offset = dst.getOffset(), start = dst.getPosition();
        dst     .put((byte) ((val >> 56) ^ 0x80))
                .put((byte) (val >> 48))
                .put((byte) (val >> 40))
                .put((byte) (val >> 32))
                .put((byte) (val >> 24))
                .put((byte) (val >> 16))
                .put((byte) (val >> 8))
                .put((byte) (long) val);
        order.apply(dst.getBytes(), offset + start, 8);
        return 8;
    }
}