package org.apache.lucene.search;

import org.apache.lucene.index.LeafReaderContext;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

class IntervalQuery extends Query {

  private final Query delegate;

  IntervalQuery(Query delegate) {
    this.delegate = delegate;
  }

  @Override
  public Query rewrite(IndexSearcher searcher) throws IOException {
    Query rewritten = delegate.rewrite(searcher);
    if (rewritten != delegate) {
      return new IntervalQuery(rewritten);
    } else {
      return this;
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    Weight inWeight = delegate.createWeight(searcher, scoreMode, boost);
    return new FilterWeight(inWeight) {

      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext leaf) throws IOException {
        final ScorerSupplier inSupplier = in.scorerSupplier(leaf);
        if (inSupplier == null) {
          return null;
        }
        return new ScorerSupplier() {
          @Override
          public Scorer get(long leadCost) throws IOException {
            return wrapScorer(inSupplier.get(leadCost), leaf);
          }

          @Override
          public long cost() {
            return inSupplier.cost();
          }
        };
      }

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        Scorer inScorer = inWeight.scorer(context);
        if (inScorer == null) {
          return null;
        }
        assert inScorer.docID() == -1;
        return wrapScorer(inScorer, context);
      }

      private Scorer wrapScorer(Scorer inScorer, LeafReaderContext context) {
        int intervalStart = context.intervalStart;
        int intervalEnd = context.intervalEnd;
        BoundedDocIdSetIterator iterator;
        final DocIdSetIterator inIterator = inScorer.iterator();
        if (inIterator == null) {
          iterator = null;
        } else {
          iterator = new BoundedDocIdSetIterator(intervalStart, intervalEnd, inIterator);
        }

        return new Scorer(this) {

          @Override
          public int docID() {
            return iterator.docID();
          }

          @Override
          public DocIdSetIterator iterator() {
            return iterator;
          }

          @Override
          public float score() throws IOException {
            return inScorer.score();
          }

          // nocommit: override twoPhaseIterator
          // this is broken, probably asDocIdSetIterator is not getting the approximation
          /*public TwoPhaseIterator twoPhaseIterator() {
            TwoPhaseIterator inTwoPhase = inScorer.twoPhaseIterator();
            if (inTwoPhase == null) {
              return null;
            }
            // FIXME - is this asDISI correct?
            return new TwoPhaseIterator(new BoundedDocIdSetIterator(intervalStart, intervalEnd,
                    TwoPhaseIterator.asDocIdSetIterator(inTwoPhase))) {
              @Override
              public boolean matches() throws IOException {
                //System.out.println("matches " + docID() + " [" + intervalStart + "," + intervalEnd + ")");
                if (docID() >= intervalEnd) {
                  return false;
                }
                return inTwoPhase.matches();
              }

              @Override
              public float matchCost() {
                return inTwoPhase.matchCost();
              }
            };
          }*/

          public int advanceShallow(int target) throws IOException {
            if (target >= intervalEnd) {
              return DocIdSetIterator.NO_MORE_DOCS;
            } else {
              return inScorer.advanceShallow(target);
            }
          }

          @Override
          public float getMaxScore(int upTo) throws IOException {
            return inScorer.getMaxScore(Math.min(upTo, intervalEnd));
          }

          /**
           * Returns child sub-scorers positioned on the current document
           *
           * @lucene.experimental
           */
          public Collection<ChildScorable> getChildren() throws IOException {
            return Collections.singleton(new ChildScorable(inScorer, "MUST"));
          }
        };
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        // nocommit how should this interact with the cache? does the ctx key need to take into account the slicing?
        return false;
      }
    };
  }

  @Override
  public String toString(String field) {
    return "IntervalQuery{" + delegate.toString() + "}";
  }

  @Override
  public void visit(QueryVisitor visitor) {
    delegate.visit(visitor);
  }

  @Override
  public boolean equals(Object obj) {
     if (!sameClassAs(obj)) {
      return false;
    }
    IntervalQuery that = (IntervalQuery) obj;
    return delegate.equals(that.delegate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(classHash(), delegate);
  }
}
