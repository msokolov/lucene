package org.apache.lucene.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.tests.search.RandomApproximationQuery;
import org.apache.lucene.tests.search.SearchEquivalenceTestBase;
import org.apache.lucene.search.BooleanClause.Occur;

public class TestIntervalQuery extends SearchEquivalenceTestBase {

  public void testConjunction() throws Exception {
    Term t1 = randomTerm();
    Term t2 = randomTerm();
    TermQuery q1 = new TermQuery(t1);
    TermQuery q2 = new TermQuery(t2);

    BooleanQuery.Builder bq1 = new BooleanQuery.Builder();
    bq1.add(q1, Occur.MUST);
    bq1.add(q2, Occur.MUST);

    BooleanQuery.Builder bq2 = new BooleanQuery.Builder();
    bq2.add(new RandomApproximationQuery(q1, random()), Occur.MUST);
    bq2.add(new RandomApproximationQuery(q2, random()), Occur.MUST);

    IntervalQuery iq1 = new IntervalQuery(bq1.build());
    assertSameScores(iq1, bq2.build());
  }

  public void testToString() {
    Term t1 = randomTerm();
    TermQuery tq = new TermQuery(t1);
    assertEquals("IntervalQuery{" + tq.toString("f") + "}", new IntervalQuery(tq).toString("f"));
  }
}
