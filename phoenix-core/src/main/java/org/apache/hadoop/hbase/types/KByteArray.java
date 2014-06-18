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

import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;
import org.apache.phoenix.schema.KTypeEncoder;

import java.util.Arrays;

/**
 * TODO we don't properly handle DESCENDING rlz encoded byte arrays yet.
 */
public class KByteArray implements DataType<byte[]> {
    static KTypeEncoder kte = new KTypeEncoder();
    protected final Order order;

    public static final KByteArray ASCENDING = new KByteArray(Order.ASCENDING);
    public static final KByteArray DESCENDING = new KByteArray(Order.DESCENDING);

    protected KByteArray() { this.order = Order.ASCENDING; }
    protected KByteArray(Order order) { this.order = (order == null) ? Order.ASCENDING : order; }

    @Override
    public boolean isOrderPreserving() {
        return true;
    }

    @Override
    public Order getOrder() {
        return null;
    }

    @Override
    public boolean isNullable() {
        return true;
    }

    @Override
    public boolean isSkippable() {
        return true;
    }

    @Override
    public int encodedLength(byte[] bytes) {
        return kte.rleEncode(null, new SimplePositionedByteRange(bytes), bytes.length);
    }

    @Override
    public Class<byte[]> encodedClass() {
        return null;
    }

    @Override
    public int skip(PositionedByteRange positionedByteRange) {
        return 0;
    }

    @Override
    public byte[] decode(PositionedByteRange src) {
        int pos = src.getPosition();
        // Problem: How do we handle reverse order using the rlz encoding?

        // "" would encode to 00 00 and needs to be last.  Null would have to be handled at the struct level.

        // option 1: we could ~ the value so that if descending "" would encode as ff ff.
        // "\x00" would encode as ff fe.   "\x00\x00" would encode as ff fd.  This should just work.

        // option 2: ~ the value before but use 00 00 as end markers.  on descending case, we'd need to special case
        // the terminator. yuk!

        // TODO implement option 1.

        // calculate the decoded length
        int dstSz = kte.rleDecode(null, src, src.getLength());
        // reset read position.  Assumes that src is not being modified by another thread
        src.setPosition(pos);

        byte[] dst = new byte[dstSz];
        PositionedByteRange dstPbr = new SimplePositionedByteRange(dst);
        kte.rleDecode(dstPbr, src, src.getLength());

        switch (order) {
            case ASCENDING:
                // NOTE: this isn't the most efficient possible way to do it but a side effect of the api.
                return dst;
            case DESCENDING:
                order.apply(dst, 0, dst.length);
                return dst;
            default:
                throw new IllegalStateException("Unexpected Order " + order);
        }

    }

    @Override
    public int encode(PositionedByteRange dst, byte[] bytes) {
        byte[] buf = bytes;
        switch(order) {
            case ASCENDING: // no need to copy if ascending
                break;
            case DESCENDING: // need to flip bits on a copy
                buf = Arrays.copyOf(buf, buf.length);
                break;
        }

        PositionedByteRange src = new SimplePositionedByteRange(buf);
        return kte.rleEncode(dst, src, buf.length);
    }
}
