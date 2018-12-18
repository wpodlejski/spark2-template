package exercises

import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.desc
import spark_helpers.SessionBuilder

object exercise2 {
  def exec1(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val sample_07 =  spark.read
      .option("delimiter", "\t")
      .csv("/home/formation/Bureau/cloudera/big-data/Spark/data/sample_07")
      .select($"_c0".as("code"),$"_c1".as("description"),$"_c2".as("total_emp"),$"_c3".as("salary"))

    val sample_08 =  spark.read
      .option("delimiter", "\t")
      .csv("/home/formation/Bureau/cloudera/big-data/Spark/data/sample_08")
      .select($"_c0".as("code"),$"_c1".as("description"),$"_c2".as("total_emp"),$"_c3".as("salary"))

    // Question 1
    //

    //Question 2
    sample_07.filter($"salary" > 100000).orderBy(desc("salary")).show

    //Question 3
    sample_07.join(sample_08, sample_07("code") === sample_08("code"))
      .select(sample_07("description"), ((sample_08("salary") - sample_07("salary"))/ sample_07("salary")).as("salary_growth"))
      .filter($"salary_growth" > 0)
      .orderBy($"salary_growth".desc).show

    //Question 4
    sample_07.join(sample_08, sample_07("code") === sample_08("code"))
      .select(sample_07("description"), ((sample_08("total_emp") - sample_07("total_emp")) * 100/ sample_07("total_emp")).as("jobs_loss(%)"))
      .filter($"jobs_loss(%)" < 0)
      .orderBy($"jobs_loss(%)").show

  }
}
