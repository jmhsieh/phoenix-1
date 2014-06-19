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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;

import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * This class both tests and demonstrates how to construct compound rowkeys
 * from a POJO. The code under test is {@link Struct}.
 */
@RunWith(Parameterized.class)
@Category(SmallTests.class)
public class TestKStruct {

  private KStruct generic;
  private Class pojoCls;
  private Object[][] constructorArgs;

  public TestKStruct(KStruct generic, Class pojoCls,
                     Object[][] constructorArgs) {
    this.generic = generic;
    this.pojoCls = pojoCls;
    this.constructorArgs = constructorArgs;
  }

  @Parameters
  public static Collection<Object[]> params() {
    Object[][] pojo1Args = {
        new Object[] { "foo", 5,   10.001 },
        new Object[] { "foo", 100, 7.0    },
        new Object[] { "foo", 100, 10.001 },
        new Object[] { "bar", 5,   10.001 },
        new Object[] { "bar", 100, 10.001 },
        new Object[] { "baz", 5,   10.001 },
    };

    Object[][] pojo2Args = {
        new Object[] { new byte[0], "it".getBytes(), "was", "the".getBytes() },
        new Object[] { "best".getBytes(), new byte[0], "of", "times,".getBytes() },
        new Object[] { "it".getBytes(), "was".getBytes(), "", "the".getBytes() },
        new Object[] { "worst".getBytes(), "of".getBytes(), "times,", new byte[0] },
        new Object[] { new byte[0], new byte[0], "", new byte[0] },
    };

    Object[][] params = new Object[][] {
        { GENERIC_POJO1, Pojo1.class, pojo1Args },
        { GENERIC_POJO2, Pojo2.class, pojo2Args },
    };
    return Arrays.asList(params);
  }

  static final Comparator<byte[]> NULL_SAFE_BYTES_COMPARATOR =
      new Comparator<byte[]>() {
        @Override
        public int compare(byte[] o1, byte[] o2) {
          if (o1 == o2) return 0;
          if (null == o1) return -1;
          if (null == o2) return 1;
          return Bytes.compareTo(o1, o2);
        }
      };

  /**
   * A simple object to serialize.
   */
  private static class Pojo1 implements Comparable<Pojo1> {
    final String stringFieldAsc;
    final int intFieldAsc;
    final double doubleFieldAsc;
    final transient String str;

    public Pojo1(Object... argv) {
      stringFieldAsc = (String) argv[0];
      intFieldAsc = (Integer) argv[1];
      doubleFieldAsc = (Double) argv[2];
      str = new StringBuilder()
            .append("{ ")
            .append(null == stringFieldAsc ? "" : "\"")
            .append(stringFieldAsc)
            .append(null == stringFieldAsc ? "" : "\"").append(", ")
            .append(intFieldAsc).append(", ")
            .append(doubleFieldAsc)
            .append(" }")
            .toString();
    }

    @Override
    public String toString() {
      return str;
    }

    @Override
    public int compareTo(Pojo1 o) {
      int cmp = stringFieldAsc.compareTo(o.stringFieldAsc);
      if (cmp != 0) return cmp;
      cmp = Integer.valueOf(intFieldAsc).compareTo(Integer.valueOf(o.intFieldAsc));
      if (cmp != 0) return cmp;
      return Double.compare(doubleFieldAsc, o.doubleFieldAsc);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (null == o) return false;
      if (!(o instanceof Pojo1)) return false;
      Pojo1 that = (Pojo1) o;
      return 0 == this.compareTo(that);
    }
  }

  /**
   * A simple object to serialize.
   *
   * TODO converted to all ascending for now.
   */
  private static class Pojo2 implements Comparable<Pojo2> {
    final byte[] byteField1Asc;
    final byte[] byteField2Dsc;
    final String stringFieldDsc;
    final byte[] byteField3Dsc;
    final transient String str;

    public Pojo2(Object... vals) {
      byteField1Asc  = vals.length > 0 ? (byte[]) vals[0] : null;
      byteField2Dsc  = vals.length > 1 ? (byte[]) vals[1] : null;
      stringFieldDsc = vals.length > 2 ? (String) vals[2] : null;
      byteField3Dsc  = vals.length > 3 ? (byte[]) vals[3] : null;
      str = new StringBuilder()
            .append("{ ")
            .append(Bytes.toStringBinary(byteField1Asc)).append(", ")
            .append(Bytes.toStringBinary(byteField2Dsc)).append(", ")
            .append(null == stringFieldDsc ? "" : "\"")
            .append(stringFieldDsc)
            .append(null == stringFieldDsc ? "" : "\"").append(", ")
            .append(Bytes.toStringBinary(byteField3Dsc))
            .append(" }")
            .toString();
    }

    @Override
    public String toString() {
      return str;
    }

    @Override
    public int compareTo(Pojo2 o) {
      int cmp = NULL_SAFE_BYTES_COMPARATOR.compare(byteField1Asc, o.byteField1Asc);
      if (cmp != 0) return cmp;

      //cmp = -NULL_SAFE_BYTES_COMPARATOR.compare(byteField2Dsc, o.byteField2Dsc);
      cmp = NULL_SAFE_BYTES_COMPARATOR.compare(byteField2Dsc, o.byteField2Dsc);
      if (cmp != 0) return cmp;

      if (stringFieldDsc == o.stringFieldDsc) cmp = 0;
      else if (null == stringFieldDsc) cmp = -1;
      else if (null == o.stringFieldDsc) cmp = 1;
      else cmp = stringFieldDsc.compareTo(o.stringFieldDsc);
      if (cmp != 0) return cmp;

      return NULL_SAFE_BYTES_COMPARATOR.compare(byteField3Dsc, o.byteField3Dsc);

//        if (stringFieldDsc == o.stringFieldDsc) cmp = 0;
//      else if (null == stringFieldDsc) cmp = 1;
//      else if (null == o.stringFieldDsc) cmp = -1;
//      else cmp = -stringFieldDsc.compareTo(o.stringFieldDsc);
//
//      if (cmp != 0) return cmp;
//      return -NULL_SAFE_BYTES_COMPARATOR.compare(byteField3Dsc, o.byteField3Dsc);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (null == o) return false;
      if (!(o instanceof Pojo2)) return false;
      Pojo2 that = (Pojo2) o;
      return 0 == this.compareTo(that);
    }
  }

    // POJO 1
    private static final KRawString stringField = new KRawString();
    private static final RawInteger intField = new RawInteger();
    private static final RawDouble doubleField = new RawDouble();
    public static KStruct GENERIC_POJO1 = new KStructBuilder().add(stringField)
          .add(intField)
          .add(doubleField)
          .toStruct();


    // POJO 2
    // TODO handle ASC and DESC encodings types.
    private static KByteArray byteField1 = new KByteArray();
    private static KByteArray byteField2 =new KByteArray();
    private static KByteArray byteField3 = new KByteArray();
    /**
     * The {@link Struct} equivalent of this type.
     */
    public static KStruct GENERIC_POJO2 =
            new KStructBuilder().add(byteField1)
                    .add(byteField2)
                    .add(stringField)
                    .add(byteField3)
                    .toStruct();


  @Test
  @SuppressWarnings("unchecked")
  public void testOrderPreservation() throws Exception {
    Object[] vals = new Object[constructorArgs.length];
    PositionedByteRange[] encodedGeneric = new PositionedByteRange[constructorArgs.length];
    Constructor<?> ctor = pojoCls.getConstructor(Object[].class);
    for (int i = 0; i < vals.length; i++) {
      vals[i] = ctor.newInstance(new Object[] { constructorArgs[i] });
      encodedGeneric[i] = new SimplePositionedByteRange(generic.encodedLength(constructorArgs[i]));
    }

    // populate our arrays
    for (int i = 0; i < vals.length; i++) {
      generic.encode(encodedGeneric[i], constructorArgs[i]);
      encodedGeneric[i].setPosition(0);
    }

    Arrays.sort(vals);
    Arrays.sort(encodedGeneric);

    for (int i = 0; i < vals.length; i++) {
      assertEquals(
        "Struct encoder does not preserve sort order at position " + i,
        vals[i],
        ctor.newInstance(new Object[] { generic.decode(encodedGeneric[i]) }));
    }
  }
}
