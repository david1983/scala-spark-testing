import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalatest.FunSuite

class RDDComparisonTest extends FunSuite with SharedSparkContext with RDDComparisons {

  test("test RDDComparisons") {
    val expectedRDD = sc.parallelize(Seq(1, 2, 3))
    val resultRDD = sc.parallelize(Seq(3, 2, 1))

    assert(None === compareRDD(expectedRDD, resultRDD)) // succeed
    assert(None !== compareRDDWithOrder(expectedRDD, resultRDD)) // Fail


//    assertRDDEquals(expectedRDD, resultRDD) // succeed
//    assertRDDEqualsWithOrder(expectedRDD, resultRDD) // Fail
  }
}
