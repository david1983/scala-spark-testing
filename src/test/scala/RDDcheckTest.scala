import com.holdenkarau.spark.testing.{RDDGenerator, SharedSparkContext}
import org.scalacheck.{Arbitrary, Gen, Prop}
import org.scalacheck.Prop.forAll
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers


class RDDcheckTest extends FunSuite with SharedSparkContext with Checkers {
  test("simple test with scalacheck") {
    val propConcatLists = forAll { (l1: List[Int], l2: List[Int]) =>
      l1.size + l2.size == (l1 ::: l2).size }
    check(propConcatLists)
  }

  test("testing sum with scalacheck") {
    val propSum = forAll { (x: Int, y: Int) =>
      prog.Main.Sum(x, y) == x + y
    }
    check(propSum)
  }

  test("generating small integers"){

    val smallInteger = Gen.choose(0,100)

    val propSmallInteger = Prop.forAll(smallInteger) { n =>
      n >= 0 && n <= 100
    }

    check(propSmallInteger)
  }

  test("generating a fixed size RDD"){
//    val rdds = RandomRDDs.exponentialRDD(sc,100,200,5,10)
//    info(rdds.toDebugString)
//    val data = rdds.collect()

    implicit val generatorDrivenConfig =
      PropertyCheckConfig(minSize = 10, maxSize = 20)
    val prop = forAll(RDDGenerator.genRDD[String](sc)(Arbitrary.arbitrary[String])){
      rdd => rdd.count() <= 20
    }

    check(prop)

  }


  test("generating a RDD based on a case class schema"){
      val property =
        forAll(RDDGenerator.genRDD[Person](sc) {
          val generator: Gen[Person] = for {
            name <- Arbitrary.arbitrary[String]
            age <- Arbitrary.arbitrary[Int]
          } yield (Person(name, age))

          generator
        }) {
          rdd => rdd.map(_.age).count() == rdd.count()
        }

      check(property)
  }

}


case class Person(name: String, age: Int)
