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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.PositionedByteRange;

/**
 * <p>
 * {@code Struct} is a simple {@link DataType} for implementing "compound
 * rowkey" and "compound qualifier" schema design strategies.
 * </p>
 * <h3>Encoding</h3>
 * <p>
 * {@code Struct} member values are encoded onto the target byte[] in the order
 * in which they are declared. A {@code Struct} may be used as a member of
 * another {@code Struct}. {@code Struct}s are not {@code nullable} but their
 * component fields may be.
 * </p>
 * <h3>Trailing Nulls</h3>
 * <p>
 * {@code Struct} treats the right-most nullable field members as special.
 * Rather than writing null values to the output buffer, {@code Struct} omits
 * those records all together. When reading back a value, it will look for the
 * scenario where the end of the buffer has been reached but there are still
 * nullable fields remaining in the {@code Struct} definition. When this
 * happens, it will produce null entries for the remaining values. For example:
 * <pre>
 * StructBuilder builder = new StructBuilder()
 *     .add(OrderedNumeric.ASCENDING) // nullable
 *     .add(OrderedString.ASCENDING)  // nullable
 * Struct shorter = builder.toStruct();
 * Struct longer = builder.add(OrderedNumeric.ASCENDING) // nullable
 *     .toStruct();
 *
 * PositionedByteRange buf1 = new SimplePositionedByteRange(7);
 * PositionedByteRange buf2 = new SimplePositionedByteRange(7);
 * Object[] val = new Object[] { BigDecimal.ONE, "foo" };
 * shorter.encode(buf1, val); // write short value with short Struct
 * buf1.setPosition(0); // reset position marker, prepare for read
 * longer.decode(buf1); // => { BigDecimal.ONE, "foo", null } ; long Struct reads implied null
 * longer.encode(buf2, val); // write short value with long struct
 * Bytes.equals(buf1.getBytes(), buf2.getBytes()); // => true; long Struct skips writing null
 * </pre>
 * </p>
 * <h3>Sort Order</h3>
 * <p>
 * {@code Struct} instances sort according to the composite order of their
 * fields, that is, left-to-right and depth-first. This can also be thought of
 * as lexicographic comparison of concatenated members.
 * </p>
 * <p>
 * {@link KStructIterator} is provided as a convenience for consuming the
 * sequence of values. Users may find it more appropriate to provide their own
 * custom {@link DataType} for encoding application objects rather than using
 * this {@code Object[]} implementation. Examples are provided in test.
 * </p>
 * <p>
 *     Struct must have at least two DataTypes in it.
 * </p>
 * @see KStructIterator
 * @see DataType#isNullable()
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class KStruct implements DataType<Object[]> {

  @SuppressWarnings("rawtypes")
  protected final DataType[] fields;
  protected final boolean isOrderPreserving;
  protected final boolean isSkippable;

  /**
   * Create a new {@code Struct} instance defined as the sequence of
   * {@code HDataType}s in {@code memberTypes}.
   * <p>
   * A {@code Struct} is {@code orderPreserving} when all of its fields
   * are {@code orderPreserving}. A {@code Struct} is {@code skippable} when
   * all of its fields are {@code skippable}.
   * </p>
   */
  @SuppressWarnings("rawtypes")
  public KStruct(DataType[] memberTypes) {
    this.fields = memberTypes;
    // a Struct is not orderPreserving when any of its fields are not.
    boolean preservesOrder = true;
    // a Struct is not skippable when any of its fields are not.
    boolean skippable = true;
    for (int i = 0; i < this.fields.length; i++) {
      DataType dt = this.fields[i];
      if (!dt.isOrderPreserving()) preservesOrder = false;
//      NOTE: with tag encoding this is no longer a constraint.
//      if (i < this.fields.length - 2 && !dt.isSkippable()) {
//        throw new IllegalArgumentException("Field in position " + i
//          + " is not skippable. Non-right-most struct fields must be skippable.");
//      }
      if (!dt.isSkippable()) skippable = false;
    }
    this.isOrderPreserving = preservesOrder;
    this.isSkippable = skippable;
  }

  @Override
  public boolean isOrderPreserving() { return isOrderPreserving; }

  @Override
  public Order getOrder() { return null; }

  @Override
  public boolean isNullable() { return false; }

  @Override
  public boolean isSkippable() { return isSkippable; }

  @SuppressWarnings("unchecked")
  @Override
  public int encodedLength(Object[] val) {
    assert fields.length >= val.length;
    int sum = 0;
    for (int i = 0; i < val.length; i++) {
      if (val[i] == null) {
          continue;
      }
      sum++; // tag
      sum += fields[i].encodedLength(val[i]);
    }
    return sum;
  }

  @Override
  public Class<Object[]> encodedClass() { return Object[].class; }

  /**
   * Retrieve an {@link Iterator} over the values encoded in {@code src}.
   * {@code src}'s position is consumed by consuming this iterator.
   */
  public KStructIterator iterator(PositionedByteRange src) {
    return new KStructIterator(src, fields);
  }

  @Override
  public int skip(PositionedByteRange src) {
    KStructIterator it = iterator(src);
    int skipped = 0;
    while (it.hasNext())
      skipped += it.skip();
    return skipped;
  }

  @Override
  public Object[] decode(PositionedByteRange src) {
    int i = 0;
    Object[] ret = new Object[fields.length];
    Iterator<Object> it = iterator(src);
    while (it.hasNext())
      ret[i++] = it.next();
    return ret;
  }

  /**
   * Read the field at {@code index}. {@code src}'s position is not affected.
   */
  public Object decode(PositionedByteRange src, int index) {
    assert index >= 0;
    KStructIterator it = iterator(src.shallowCopy());
    for (; index > 0; index--)
      it.skip();
    return it.next();
  }

  @SuppressWarnings("unchecked")
  @Override
  public int encode(PositionedByteRange dst, Object[] val) {
    if (val.length == 0) return 0;
    assert fields.length >= val.length;
    int end, written = 0;

    // We don't have this constraint any more with tags.
    // find the last occurrence of a non-null or null and non-nullable value
    for (end = val.length - 1; end > -1; end--) {
      if (null != val[end] || (null == val[end] && !fields[end].isNullable())) break;
    }
    for (int i = 0; i <= end; i++) {
      if (val[i] == null) {
        // skip null values
        continue;
      }
      // insert tag byte and then encoded bytes
      dst.put(generateTag(i, fields[i]));
      written ++;
      written += fields[i].encode(dst, val[i]);
    }
    return written;
  }

    /**
     * Phoenix assumes bytes have been converted already and deals with byte to byte operations.
     * @param dst
     * @param encVals already encoded values.
     * @return
     */
    public int encodeBytes(ByteArrayOutputStream dst, byte[][] encVals) throws IOException {
      if (encVals.length == 0) return 0;
      assert fields.length >= encVals.length;
      int end, written = 0;

      // We don't have this constraint any more with tags.
      // find the last occurrence of a non-null or null and non-nullable value
      for (end = encVals.length - 1; end > -1; end--) {
          if (null != encVals[end] || (null == encVals[end] && !fields[end].isNullable())) break;
      }
      for (int i = 0; i < end; i++) {
          if (encVals[i] == null) {
              // skip null values
              continue;
          }

          // insert tag byte and then encoded bytes
          dst.write(generateTag(i, fields[i]));
          written++;
          dst.write(encVals[i]);
          written += encVals[i].length;
      }
      return written;
  }

  byte generateTag(int pos, DataType t) {
      RawSize rawSize = null;
      if (t instanceof KByteArray || t instanceof KRawString) {
          switch (t.getOrder()) {
              case ASCENDING:
                  rawSize = RawSize.RLZBYTES;
                  break;
              case DESCENDING:
                  rawSize = RawSize.RLZBYTESDESC;
                  break;
          }
      } else if (t instanceof RawInteger || t instanceof OrderedRawInt32) {
          rawSize = RawSize.INT32;
      } else if (t instanceof RawLong) {
          rawSize = RawSize.INT64;
      } else if (t instanceof RawDouble) {
          rawSize = RawSize.INT64;
      }
      // ...
      // TODO

      if (rawSize == null) {
          // TODO
          throw new IllegalStateException("Currently cannot handle type " + t);
      }
      return encodeTag(pos, rawSize);
  }


    enum RawSize {
        PROTO(0), INT64(1), INT32(2), INT16(3), INT8(4), RLZBYTES(5), FIXEDBYTES(6), RLZBYTESDESC(7);
        private int val;
        RawSize(int val) { this.val = val; }
        public int getVal() { return val; }
    };

    /**
     * A tag preserves order for complex row keys by encoding a position ordinal and a raw storage type.
     * The most significant five bits encode position, while the remaining three encode type.  This limits a rowkey
     * to 32 fields, but allows nulls or skipped fields to be ordered after keys where the field is present.
     *
     * @param pos constrained from 0 to 31.
     * @param t raw encoding type that follows.
     * @return encoded tag value.
     */
    byte encodeTag(int pos, RawSize t) {
        Preconditions.checkPositionIndex(pos, 32, "Type position too large");
        Preconditions.checkNotNull(t, "Must specify non-null RawType for tag");
        return  (byte) ((pos << 3) | (t.getVal() & 0x7));
    }

    public static int tagPos(byte tag){
        return ((int)tag) >> 3;
    }

    public static RawSize tagType(byte tag) {
        int pos = tag >> 3;
        return RawSize.values()[pos];
    }

}
