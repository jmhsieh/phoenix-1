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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;

/**
 * New type encodings for Typed data.
 */
public class KTypeEncoder {
    enum RawType {
        INT64(1), INT32(2), INT16(3), INT8(4), RLEBYTES(5), FIXEDBYTES(6);
        private int val;
        RawType(int val) { this.val = val; }
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
    byte encodeTag(int pos, RawType t) {
        Preconditions.checkPositionIndex(pos, 32, "Type position too large");
        Preconditions.checkNotNull(t, "Must specify non-null RawType for tag");
       return  (byte) ((pos << 3) | (t.getVal() & 0x7));
    }

    int MAX_UNSIGNED_BYTE = 255;

    /**
     * Encode data from the src PBR into the dst PBR.
     *
     * @param dst destination pbr.  if null, then we don't write but still return length
     * @param src
     * @param maxLen
     * @return size of encoded bytes
     */
    public int rleEncode(PositionedByteRange dst, PositionedByteRange src, int maxLen) {
        int encSz = 0;
        byte b;

        Preconditions.checkArgument(src.getRemaining() >= maxLen, "illegal maxLen! it is larger than bytes remaining");
        for (int i = 0; i <= maxLen ; i++) {
            // nothing left, bail out
            if (src.getRemaining() <= 0) {
                break;
            }

            b = src.get();
            // normal case, byte is not 0
            if (b != 0) {
                if (dst != null)
                    dst.put(b);
                encSz++;
                continue;
            }

            // special case, byte is a zero, let's run length encode this.
            int run = 1;
            // while we still have bytes to read and the next is a 0
            while (src.getRemaining() > 0 && src.peek() == 0) {
                run++;
                i++;
                b = src.get();
                // we got to max RLE?  bail out and write.
                if (run == MAX_UNSIGNED_BYTE - 1) {
                    break;
                }
            }

            if (dst != null) {
                dst.put((byte) 0x0);
                dst.put((byte) ~run);
            }
            encSz += 2;
        }
        // end marker
        if (dst != null) {
            dst.put((byte) 0x0);
            dst.put((byte) 0x0);
        }
        encSz += 2;
        return encSz;
    }

    /**
     * Decode data from the src PBR into the dst PBR
     *
     * @param dst desitination pbr.  If null, we don't write but still return maxLen.
     * @param src
     * @param maxLen
     * @return size of decoded bytes
     */
    public int rleDecode(PositionedByteRange dst, PositionedByteRange src, int maxLen) {
        byte b = 0;
        int decSz =0;
        for (int i =0; i < maxLen; i++) {
            // nothing left, bail out.
            if (src.getRemaining() <= 0) {
                break;
            }

            b = src.get();

            // normal case, byte is not 0
            if (b != 0) {
                if (dst != null)
                    dst.put(b);
                decSz++;
                continue;
            }

            // special case, byte is a zero,let's run length decode this.
            if (src.getRemaining() <= 0) {
                throw new IllegalArgumentException("Improperly encoded RLE bytes. Didn't have encoded length!");
            }

            // get byte and force it to be unsigned.
            int run = 0xff & src.get();
            i++;

            if (run == 0) {
                // reached end of string
                break;
            }

            // invert bits in run length so that a long string of 0's encodes after fewer zeros
            //      raw bytes    rle'd         rle+invert
            // raw: 00 00 00  -> 00 "03"    => 00 fc     (order preserved)
            // raw: 00 00 01  -> 00 "02" 01 => 00 fd 01
            run = 0xff & (~run);

            for (int j = 0; j < run; j++) {
                if (dst != null)
                    dst.put((byte)0);
                decSz++;
            }
        }
        return decSz;
    }

    /**
     * TODO this is an api convenient but performance inefficient helper method
     * @param src
     * @return
     */
    byte[] rleEncode(byte[] src) {
        PositionedByteRange srcPbr = new SimplePositionedByteRange(src);
        PositionedByteRange dstPbr = new SimplePositionedByteRange(new byte[src.length*2+2]);
        int dstSz = rleEncode(dstPbr, srcPbr, src.length);
        byte[] tmp = new byte[dstSz];
        dstPbr.deepCopySubRangeTo(0, dstSz, tmp, 0);
        return tmp;
    }

    /**
     * TODO this is an api convenient but performance inefficient helper method
     * @param src
     * @return
     */
    byte[] rleDecode(byte[] src, int offset, int length) {
        PositionedByteRange srcPbr = new SimplePositionedByteRange(src, offset, length);
        // wow, this could example 255x!
        PositionedByteRange dstPbr = new SimplePositionedByteRange(new byte[src.length*255]);
        int dstSz = rleDecode(dstPbr, srcPbr, src.length);
        byte[] tmp = new byte[dstSz];
        dstPbr.deepCopySubRangeTo(0, dstSz, tmp, 0);
        return tmp;
    }


}
