import com.holdenkarau.spark.testing.{Column, DataframeGenerator, SharedSparkContext}
import org.apache.spark.sql.{SQLContext}
import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers
import org.scalacheck.{Gen}
import org.scalacheck.Prop.forAll

class DataFrameCheckTest extends FunSuite with SharedSparkContext with Checkers {
  test("assert dataframes generated correctly") {
    val schema = StructType(List(StructField("name", StringType), StructField("age", IntegerType)))
    val sqlContext = new SQLContext(sc)
    val dataframeGen = DataframeGenerator.arbitraryDataFrame(sqlContext, schema)

    val property =
      forAll(dataframeGen.arbitrary) {
        dataframe => dataframe.schema === schema && dataframe.count >= 0
      }

    check(property)
  }

  test("test multiple columns generators") {
    val schema = StructType(List(StructField("name", StringType), StructField("age", IntegerType)))
    val sqlContext = new SQLContext(sc)
    val nameGenerator = new Column("name", Gen.oneOf("Holden", "Hanafy")) // name should be one of those
    val ageGenerator = new Column("age", Gen.choose(10, 100))
    val dataframeGen = DataframeGenerator.arbitraryDataFrameWithCustomFields(sqlContext, schema)(nameGenerator, ageGenerator)

    val property =
      forAll(dataframeGen.arbitrary) {
        dataframe => dataframe.schema === schema &&
          dataframe.filter("(name != 'Holden' AND name != 'Hanafy') OR (age > 100 OR age < 10)").count() == 0
      }

    check(property)
  }

}
