package exercises

import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.min
import spark_helpers.SessionBuilder

object exercise3 {
  def exec1(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    toursDF.show

    //Question 1
    toursDF.select($"tourDifficulty")
      .distinct()
      .show()

    //Question 2
    toursDF.agg(min($"tourPrice").as("priceMin"), max($"tourPrice").as("priceMax"),avg($"tourPrice").as("averagePrice"))
      .show()

    //Question 3
    toursDF.groupBy($"tourDifficulty")
      .agg(min($"tourPrice").as("priceMin"), max($"tourPrice").as("priceMax"),avg($"tourPrice").as("averagePrice"))
      .show()

    //Question 4
    toursDF.groupBy($"tourDifficulty")
      .agg(min($"tourPrice").as("priceMin"), max($"tourPrice").as("priceMax"),avg($"tourPrice").as("averagePrice"),min($"tourLength").as("durationMin"), max($"tourLength").as("durationMax"),avg($"tourLength").as("averageDuration"))
      .show()

    //Question 5
    toursDF.select(explode($"tourTags"))
      .groupBy($"col")
      .count()
      .orderBy($"count".desc).
      show(10)

    //Question 6
    toursDF
      .select(explode($"tourTags"), $"tourDifficulty")
      .groupBy($"col", $"tourDifficulty")
      .count()
      .orderBy($"count".desc).
      show(10)

    //Question 7
    toursDF
      .select(explode($"tourTags").as("tags"), $"tourDifficulty",$"tourPrice")
      .groupBy($"tags", $"tourDifficulty")
      .agg(min($"tourPrice").as("priceMin"), max($"tourPrice").as("priceMax"),avg($"tourPrice").as("averagePrice"))
      .orderBy($"averagePrice".desc)
      .show(10)



  }

}
