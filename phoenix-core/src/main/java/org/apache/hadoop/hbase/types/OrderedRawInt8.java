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
 * An {@code int} of 32-bits using a fixed-length encoding. Built on
 * {@link org.apache.hadoop.hbase.util.OrderedBytes#encodeInt32(org.apache.hadoop.hbase.util.PositionedByteRange, int, org.apache.hadoop.hbase.util.Order)}.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class OrderedRawInt8 extends OrderedBytesBase<Byte> {

    public static final OrderedRawInt8 ASCENDING = new OrderedRawInt8(Order.ASCENDING);
    public static final OrderedRawInt8 DESCENDING = new OrderedRawInt8(Order.DESCENDING);

    public OrderedRawInt8(Order order) { super(order); }

    @Override
    public boolean isNullable() { return false; }

    @Override
    public int encodedLength(Byte val) { return 1; }

    @Override
    public Class<Byte> encodedClass() { return Byte.class; }

    @Override
    public Byte decode(PositionedByteRange src) {
        return (byte)((order.apply(src.get()) ^ 0x80) & 0xff);
    }

    @Override
    public int encode(PositionedByteRange dst, Byte val) {
        if (null == val) throw new IllegalArgumentException("Null values not supported.");
        // if no destination, just return size.
        if (dst == null) return 1;

        final int offset = dst.getOffset(), start = dst.getPosition();
        dst     .put((byte) (val ^ 0x80));
        order.apply(dst.getBytes(), offset + start, 1);
        return 1;
    }

}