package exercises

import org.apache.spark.sql.functions.sum
import spark_helpers.SessionBuilder

object exercise1 {
  def exec1(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val demography = spark.read.json("data/input/demographie_par_commune.json")
    val departement =  spark.read
      .csv("/home/formation/Bureau/cloudera/big-data/Spark/data/departements.txt")
      .select($"_c0".as("departement"),$"_c1".as("code"))

    // Question 1
    demography.agg(sum($"Population").as("Total_population")).show

    //Question 2
    demography.groupBy($"Departement").agg(sum($"Population")).show

    //Question 3
    departement.join(demography, departement("code") === demography("Departement"))
      .groupBy(departement("departement")).agg(sum(demography("Population")).as("total"))
      .orderBy($"total".desc).show

  }

}
