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

package org.apache.lucene.analysis.synonym.word2vec;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.TermAndVector;

/**
 * Word2VecModel is a class representing the parsed Word2Vec model containing the vectors for each
 * word in dictionary
 *
 * @lucene.experimental
 */
public class Word2VecModel extends FloatVectorValues {

  private final int dictionarySize;
  private final int vectorDimension;
  private final TermAndVector[] termsAndVectors;
  private final BytesRefHash word2Vec;
  private int loadedCount = 0;

  public Word2VecModel(int dictionarySize, int vectorDimension) {
    this.dictionarySize = dictionarySize;
    this.vectorDimension = vectorDimension;
    this.termsAndVectors = new TermAndVector[dictionarySize];
    this.word2Vec = new BytesRefHash();
  }

  public void addTermAndVector(TermAndVector modelEntry) {
    modelEntry = modelEntry.normalizeVector();
    this.termsAndVectors[loadedCount++] = modelEntry;
    this.word2Vec.add(modelEntry.term());
  }

  @Override
  public Floats vectors() {
    return new Floats() {
      @Override
      public float[] get(int targetOrd) {
        return termsAndVectors[targetOrd].vector();
      }
    };
  }

  public float[] vectorValue(BytesRef term) {
    int termOrd = this.word2Vec.find(term);
    if (termOrd < 0) return null;
    TermAndVector entry = this.termsAndVectors[termOrd];
    return (entry == null) ? null : entry.vector();
  }

  public BytesRef termValue(int targetOrd) {
    return termsAndVectors[targetOrd].term();
  }

  @Override
  public int dimension() {
    return vectorDimension;
  }

  @Override
  public int size() {
    return dictionarySize;
  }
}
