package org.apache.lucene.search;

import java.io.IOException;

/**
 * A doc ID set iterator that wraps a delegate iterator and only returns doc IDs in the range
 * [firstDocInclusive, lastDoc).
 */
class BoundedDocIdSetIterator extends DocIdSetIterator {
  private final int firstDoc;
  private final int lastDoc;
  private final DocIdSetIterator delegate;

  private int docID = -1;

  BoundedDocIdSetIterator(int firstDoc, int lastDoc, DocIdSetIterator delegate) {
    assert delegate != null;
    this.firstDoc = firstDoc;
    this.lastDoc = lastDoc;
    this.delegate = delegate;
  }

  @Override
  public int docID() {
    return docID;
  }

  @Override
  public int nextDoc() throws IOException {
    return advance(docID + 1);
  }

  @Override
  public int advance(int target) throws IOException {
    if (target < firstDoc) {
      target = firstDoc;
    }

    int result = delegate.advance(target);
    if (result < lastDoc) {
      docID = result;
    } else {
      docID = NO_MORE_DOCS;
    }
    return docID;
  }

  @Override
  public long cost() {
    return Math.min(delegate.cost(), lastDoc - firstDoc);
  }
}
