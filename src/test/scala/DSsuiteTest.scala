import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.scalatest.FunSuite

class DSsuiteTest extends FunSuite with DatasetSuiteBase {
  test("testing DataSet equality") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val input1 = sc.parallelize(List(1, 2, 3)).toDS
    assertDatasetEquals(input1, input1) // equal

    val input2 = sc.parallelize(List(4, 5, 6)).toDS
    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDatasetEquals(input1, input2) // not equal
    }
  }

  test("testing DataSet approximate equality") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val input1 = sc.parallelize(List[(Int, Double)]((1, 1.1), (2, 2.2), (3, 3.3))).toDS
    val input2 = sc.parallelize(List[(Int, Double)]((1, 1.2), (2, 2.3), (3, 3.4))).toDS
    assertDatasetApproximateEquals(input1, input2, 0.11) // equal

    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDatasetApproximateEquals(input1, input2, 0.05) // not equal
    }
  }
}