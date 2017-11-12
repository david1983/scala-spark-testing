package prog

import org.apache.spark.sql.{DataFrame, SparkSession}

object Main extends App{
  val currentDirectory = new java.io.File(".").getCanonicalPath
  val spark = SparkSession
    .builder
    .master("yarn-client")
    .appName("scala-spark").getOrCreate()

  println("app starting")
  println("current dir: " + currentDirectory)


  def loadFile(fname:String): DataFrame ={
    spark.read.option("header",true).csv(currentDirectory + "/data/" + fname)
  }

  def Sum(x:Int, y:Int):Int = x+y


  def init(): Unit ={
    val df = loadFile("pokemon.csv")
    df.show()
  }

  init()

}
