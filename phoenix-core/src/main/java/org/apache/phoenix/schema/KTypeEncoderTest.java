/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.schema;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class KTypeEncoderTest {

    KTypeEncoder kte = new KTypeEncoder();

    public int testEncDec(byte[] src, byte[] exp, byte[] dst) {
        PositionedByteRange srcVal = new SimplePositionedByteRange(src);
        PositionedByteRange encVal = new SimplePositionedByteRange(dst);
        PositionedByteRange decVal= new SimplePositionedByteRange(new byte[src.length]);

        int encSz = kte.rleEncode(encVal, srcVal, src.length);
        encVal.setOffset(0); // restart the pointer
        int decSz = kte.rleDecode(decVal, encVal, encSz);

        // check that val == decVal
        Assert.assertEquals(src.length, decSz);
        byte[] tmp = new byte[decSz];
        decVal.deepCopySubRangeTo(0, decSz, tmp, 0);
        Assert.assertArrayEquals(exp, tmp);
        return encSz;
    }

    @Test
    public void timeBasics() {
        byte[] buf = new byte[500]; // just something big enough
        byte[] src = Bytes.toBytes("This is a test");
        testEncDec(src, src, buf);
    }

    /**
     * The worse case for this RLE encoding is alternating value then 0.
     *
     * Pre encode         encoded                    terminator
     * 01 00 01 00 ... -> 01 "00 ~01" 01 "00 ~01" .. 00 00
     *
     */
    @Test
    public void testAlternating() {
        byte[] src = new byte[500];
        byte[] dst = new byte[src.length *2]; // we'll always fit in 2x the space.

        // set every other value to be non-zero
        for (int i=0; i<src.length; i+=2) {
            src[i] = 1;
        }

        int encSz = testEncDec(src, src, dst);
        Assert.assertEquals(encSz, 752); //  should be rle encoded down to 750 + 2 bytes
    }

    /**
     * The max run length is 255 since we only have a byte to encode length.  Handle that case.
     */
    @Test
    public void testAllZeros() {
        byte[] src = new byte[500]; // java inits byte[]'s to all 0's
        byte[] dst = new byte[src.length *2]; // we'll always fit in 2x the space.

        int encSz = testEncDec(src, src, dst);
        Assert.assertEquals(encSz, 6); //  should be rle encoded down to 6 bytes (00 ~254 00 ~246 00 00)
    }

    /**
     * These rle strings are terminated with \x0\x0.  Make sure this is honored
     */
    @Test
    public void testTerminator() {
        byte[] enc = new byte[] { 'a', 'b', 'c', 0, 0, 'd'};
        byte[] exp = new byte[] { 'a', 'b', 'c'};
        byte[] dst = new byte[exp.length];

        PositionedByteRange encVal = new SimplePositionedByteRange(enc);
        PositionedByteRange decVal= new SimplePositionedByteRange(dst);

        int decSz = kte.rleDecode(decVal, encVal, enc.length);
        Assert.assertArrayEquals(exp, dst);
    }

    /**
     * Demonstrate that shorter rlz encoded arrays sort before, and that expected order is properly preserved.
     *
     * 00 00 01 -> 00 fe 01 00 00
     * 00 00 00 -> 00 fd 00 00
     */
    @Test
    public void testOrderPreservation() {
        byte [] srcA = new byte[] { 0, 0, 0 };
        byte [] srcB = new byte[] { 0, 0, 1 };
        boolean aLtB = Bytes.compareTo(srcA, srcB) < 0;
        boolean encALtB = Bytes.compareTo(kte.rleEncode(srcA), kte.rleEncode(srcB)) < 0;
        Assert.assertTrue(aLtB);
        Assert.assertTrue(encALtB);
    }
}
