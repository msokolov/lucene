/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.backward_codecs.lucene92;

import java.io.IOException;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.packed.DirectMonotonicReader;

/** Read the vector values from the index input. This supports both iterated and random access. */
abstract class OffHeapFloatVectorValues extends FloatVectorValues {

  protected final int dimension;
  protected final int size;
  protected final IndexInput slice;
  protected final int byteSize;
  protected final VectorSimilarityFunction vectorSimilarityFunction;

  OffHeapFloatVectorValues(
      int dimension,
      int size,
      VectorSimilarityFunction vectorSimilarityFunction,
      IndexInput slice) {
    this.dimension = dimension;
    this.size = size;
    this.slice = slice;
    byteSize = Float.BYTES * dimension;
    this.vectorSimilarityFunction = vectorSimilarityFunction;
  }

  @Override
  public int dimension() {
    return dimension;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public Floats vectors() throws IOException {
    return new Floats() {
      final IndexInput vectorSlice = slice.clone();
      int lastOrd = -1;
      float[] value = new float[dimension];

      @Override
      public float[] get(int targetOrd) throws IOException {
        if (lastOrd == targetOrd) {
          return value;
        }
        vectorSlice.seek((long) targetOrd * byteSize);
        vectorSlice.readFloats(value, 0, value.length);
        lastOrd = targetOrd;
        return value;
      }
    };
  }

  static OffHeapFloatVectorValues load(
      Lucene92HnswVectorsReader.FieldEntry fieldEntry, IndexInput vectorData) throws IOException {
    if (fieldEntry.docsWithFieldOffset() == -2) {
      return new EmptyOffHeapVectorValues(fieldEntry.dimension());
    }
    IndexInput bytesSlice =
        vectorData.slice(
            "vector-data", fieldEntry.vectorDataOffset(), fieldEntry.vectorDataLength());
    if (fieldEntry.docsWithFieldOffset() == -1) {
      return new DenseOffHeapVectorValues(
          fieldEntry.dimension(), fieldEntry.size(), fieldEntry.similarityFunction(), bytesSlice);
    } else {
      return new SparseOffHeapVectorValues(
          fieldEntry, vectorData, fieldEntry.similarityFunction(), bytesSlice);
    }
  }

  static class DenseOffHeapVectorValues extends OffHeapFloatVectorValues {

    public DenseOffHeapVectorValues(
        int dimension,
        int size,
        VectorSimilarityFunction vectorSimilarityFunction,
        IndexInput slice) {
      super(dimension, size, vectorSimilarityFunction, slice);
    }

    @Override
    public DocIndexIterator iterator() {
      return createDenseIterator();
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
      return acceptDocs;
    }

    @Override
    public VectorScorer scorer(float[] query) throws IOException {
      FloatVectorValues.Floats values = vectors();
      DocIndexIterator iterator = iterator();
      return new VectorScorer() {
        @Override
        public float score() throws IOException {
          return vectorSimilarityFunction.compare(values.get(iterator.index()), query);
        }

        @Override
        public DocIdSetIterator iterator() {
          return iterator;
        }
      };
    }
  }

  private static class SparseOffHeapVectorValues extends OffHeapFloatVectorValues {
    private final DirectMonotonicReader ordToDoc;
    private final IndexInput dataIn;
    private final Lucene92HnswVectorsReader.FieldEntry fieldEntry;

    public SparseOffHeapVectorValues(
        Lucene92HnswVectorsReader.FieldEntry fieldEntry,
        IndexInput dataIn,
        VectorSimilarityFunction vectorSimilarityFunction,
        IndexInput slice)
        throws IOException {

      super(fieldEntry.dimension(), fieldEntry.size(), vectorSimilarityFunction, slice);
      final RandomAccessInput addressesData =
          dataIn.randomAccessSlice(fieldEntry.addressesOffset(), fieldEntry.addressesLength());
      this.ordToDoc = DirectMonotonicReader.getInstance(fieldEntry.meta(), addressesData);
      this.dataIn = dataIn;
      this.fieldEntry = fieldEntry;
    }

    private IndexedDISI createDISI() throws IOException {
      return new IndexedDISI(
          dataIn.clone(),
          fieldEntry.docsWithFieldOffset(),
          fieldEntry.docsWithFieldLength(),
          fieldEntry.jumpTableEntryCount(),
          fieldEntry.denseRankPower(),
          fieldEntry.size());
    }

    @Override
    public DocIndexIterator iterator() throws IOException {
      return IndexedDISI.asDocIndexIterator(createDISI());
    }

    @Override
    public int ordToDoc(int ord) {
      return (int) ordToDoc.get(ord);
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
      if (acceptDocs == null) {
        return null;
      }
      return new Bits() {
        @Override
        public boolean get(int index) {
          return acceptDocs.get(ordToDoc(index));
        }

        @Override
        public int length() {
          return size;
        }
      };
    }

    @Override
    public VectorScorer scorer(float[] query) throws IOException {
      FloatVectorValues.Floats values = vectors();
      IndexedDISI disi = createDISI();
      return new VectorScorer() {
        @Override
        public float score() throws IOException {
          return vectorSimilarityFunction.compare(values.get(disi.index()), query);
        }

        @Override
        public DocIdSetIterator iterator() {
          return disi;
        }
      };
    }
  }

  private static class EmptyOffHeapVectorValues extends OffHeapFloatVectorValues {

    public EmptyOffHeapVectorValues(int dimension) {
      super(dimension, 0, VectorSimilarityFunction.COSINE, null);
    }

    @Override
    public int dimension() {
      return super.dimension();
    }

    @Override
    public int size() {
      return 0;
    }

    @Override
    public Floats vectors() {
      return Floats.EMPTY;
    }

    @Override
    public int ordToDoc(int ord) {
      throw new UnsupportedOperationException();
    }

    @Override
    public DocIndexIterator iterator() {
      return createDenseIterator();
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
      return null;
    }

    @Override
    public VectorScorer scorer(float[] query) {
      return null;
    }
  }
}
