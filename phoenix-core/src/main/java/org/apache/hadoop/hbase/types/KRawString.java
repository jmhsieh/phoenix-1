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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;
import org.apache.phoenix.schema.KTypeEncoder;

/**
 * An {@code DataType} for interacting with values encoded using
 * {@link Bytes#toBytes(String)}. Intended to make it easier to transition
 * away from direct use of {@link Bytes}.
 * @see Bytes#toBytes(String)
 * @see Bytes#toString(byte[])
 * @see RawStringTerminated
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class KRawString implements DataType<String> {
  public static final KRawString ASCENDING = new KRawString(Order.ASCENDING);
  public static final KRawString DESCENDING = new KRawString(Order.DESCENDING);

  protected final Order order;

  protected KRawString() { this.order = Order.ASCENDING; }
  protected KRawString(Order order) { this.order = order; }

  private KByteArray getKba() {
      switch (order) {
          case ASCENDING: return KByteArray.ASCENDING;
          case DESCENDING: return KByteArray.DESCENDING;
          default:
              throw new IllegalStateException("Unhandled Order: " + order);
      }
  }

  @Override
  public boolean isOrderPreserving() { return true; }

  @Override
  public Order getOrder() { return order; }

  @Override
  public boolean isNullable() { return false; }

  @Override
  public boolean isSkippable() { return false; }

  @Override
  public int skip(PositionedByteRange src) {
    int skipped = src.getRemaining();
    src.setPosition(src.getLength());
    return skipped;
  }

  @Override
  public int encodedLength(String val) { return getKba().encodedLength(val.getBytes()); }

  @Override
  public Class<String> encodedClass() { return String.class; }

  @Override
  public String decode(PositionedByteRange src) {
    if (Order.ASCENDING == this.order) {
      String val = Bytes.toString(getKba().decode(src));
      return val;
    } else {
      byte[] b = new byte[src.getRemaining()];
      src.get(b);
      byte[] dec = getKba().decode(new SimplePositionedByteRange(b));
      order.apply(dec, 0, dec.length);
      return Bytes.toString(dec);
    }
  }

  @Override
  public int encode(PositionedByteRange dst, String val) {
    byte[] s = Bytes.toBytes(val);
    order.apply(s);
    getKba().encode(dst, s);
    return s.length;
  }
}
