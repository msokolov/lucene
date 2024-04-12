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
package org.apache.lucene.index;

import java.util.Collections;
import java.util.List;

/** {@link IndexReaderContext} for {@link LeafReader} instances. */
public final class LeafReaderContext extends IndexReaderContext {
  /** The reader's ord in the top-level's leaves array */
  public final int ord;

  /** The reader's absolute doc base */
  public final int docBase;

  /** The range of docids spanned by this context; left-inclusive, right-exclusive. */
  public final int intervalStart, intervalEnd;

  private final LeafReader reader;
  private final List<LeafReaderContext> leaves;

  /** Creates a new {@link LeafReaderContext} */
  LeafReaderContext(
      IndexReaderContext parent,
      LeafReader reader,
      int ord,
      int docBase,
      int leafOrd,
      int leafDocBase,
      int intervalStart,
      int intervalEnd) {
    super(parent, ord, docBase);
    this.ord = leafOrd;
    this.docBase = leafDocBase;
    this.reader = reader;
    this.leaves = isTopLevel ? Collections.singletonList(this) : null;
    this.intervalStart = intervalStart;
    this.intervalEnd = intervalEnd;
  }

  /** Creates a new {@link LeafReaderContext} whose doc interval spans the entire leaf */
  LeafReaderContext(
      IndexReaderContext parent,
      LeafReader reader,
      int ord,
      int docBase,
      int leafOrd,
      int leafDocBase) {
    this(
        parent,
        reader,
        ord,
        docBase,
        leafOrd,
        leafDocBase,
        0,
        parent == null ? 0 : reader.maxDoc());
  }

  LeafReaderContext(LeafReader leafReader) {
    this(null, leafReader, 0, 0, 0, 0);
  }

  @Override
  public List<LeafReaderContext> leaves() {
    if (!isTopLevel) {
      throw new UnsupportedOperationException("This is not a top-level context.");
    }
    assert leaves != null;
    return leaves;
  }

  @Override
  public List<IndexReaderContext> children() {
    return null;
  }

  @Override
  public LeafReader reader() {
    return reader;
  }

  public LeafReaderContext createSubLeaf(int intervalStart, int intervalEnd) {
    assert intervalStart >= 0;
    assert intervalEnd > intervalStart && intervalEnd <= reader.maxDoc()
        : "[" + intervalStart + "," + intervalEnd + "] maxdoc=" + reader.maxDoc();
    LeafReaderContext parent = this;
    return new LeafReaderContext(
        parent, reader, ord, docBase, ord, docBase, intervalStart, intervalEnd);
  }

  @Override
  public String toString() {
    return "LeafReaderContext(" + reader + " docBase=" + docBase + " ord=" + ord + ")";
  }
}
