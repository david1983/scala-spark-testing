import com.holdenkarau.spark.testing.{DatasetGenerator, SharedSparkContext}
import org.apache.spark.sql.{Dataset, SQLContext}
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Prop.forAll


class DataSetGeneratorTest extends FunSuite with SharedSparkContext with Checkers {
  test("test generating Datasets[String]") {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val property =
      forAll(DatasetGenerator.genDataset[String](sqlContext)(Arbitrary.arbitrary[String])) {
        dataset => dataset.map(_.length).count() == dataset.count()
      }

    check(property)
  }

  test("test generating Datasets[Custom Class]") {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val carGen: Gen[Dataset[Car]] =
      DatasetGenerator.genDataset[Car](sqlContext) {
        val generator: Gen[Car] = for {
          name <- Arbitrary.arbitrary[String]
          speed <- Arbitrary.arbitrary[Int]
        } yield (Car(name, speed))

        generator
      }

    val property =
      forAll(carGen) {
        dataset => dataset.map(_.speed).count() == dataset.count()
      }

    check(property)
  }

}


case class Car(name: String, speed: Int)