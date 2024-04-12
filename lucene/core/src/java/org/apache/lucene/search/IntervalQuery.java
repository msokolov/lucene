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
        TwoPhaseIterator inTwoPhase = inScorer.twoPhaseIterator();
        if (inTwoPhase != null) {
          return new TwoPhaseScorer(inScorer, inTwoPhase, intervalStart, intervalEnd);
        } else {
          return new IntervalScorer(inScorer, intervalStart, intervalEnd);
        }
      }

      private class TwoPhaseScorer extends Scorer {
        private final Scorer inScorer;
        private final TwoPhaseIterator twoPhase;
        private final DocIdSetIterator iterator;
        private final int intervalEnd;

        TwoPhaseScorer(Scorer scorer, TwoPhaseIterator twoPhase, int intervalStart, int intervalEnd) {
          super(scorer.weight);
          DocIdSetIterator approx = new BoundedDocIdSetIterator(intervalStart, intervalEnd, twoPhase.approximation());
          this.twoPhase = createTwoPhase(twoPhase, approx);
          this.iterator = TwoPhaseIterator.asDocIdSetIterator(this.twoPhase);
          this.inScorer = scorer;
          this.intervalEnd = intervalEnd;
        }

        private static TwoPhaseIterator createTwoPhase(TwoPhaseIterator twoPhase, DocIdSetIterator boundedApproximation) {
          return new TwoPhaseIterator(boundedApproximation) {
            @Override
            public boolean matches() throws IOException {
              return twoPhase.matches();
            }

            @Override
            public float matchCost() {
              return twoPhase.matchCost();
            }
          };
        }

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

        @Override
        public TwoPhaseIterator twoPhaseIterator() {
          return twoPhase;
        }

        @Override
        public float getMaxScore(int upTo) throws IOException {
          return inScorer.getMaxScore(Math.min(upTo, intervalEnd));
        }

        @Override
        public Collection<ChildScorable> getChildren() throws IOException {
          return Collections.singleton(new ChildScorable(inScorer, "MUST"));
        }
      }

      private class IntervalScorer extends Scorer {
        private final Scorer inScorer;
        private final DocIdSetIterator iterator;
        private final int intervalEnd;

        IntervalScorer(Scorer inScorer, int intervalStart, int intervalEnd) {
          super(inScorer.weight);
          this.iterator = new BoundedDocIdSetIterator(intervalStart, intervalEnd, inScorer.iterator());
          this.inScorer = inScorer;
          this.intervalEnd = intervalEnd;
        }

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

        public TwoPhaseIterator twoPhaseIterator() {
          return null;
        }

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

        @Override
        public Collection<ChildScorable> getChildren() throws IOException {
          return Collections.singleton(new ChildScorable(inScorer, "MUST"));
        }
      };

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        // nocommit we must disable the cache until we can use sub-leaves as cache keys
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
