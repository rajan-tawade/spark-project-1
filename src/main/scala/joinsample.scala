import org.apache.spark.sql.SparkSession

object joinsample {

def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[1]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val df1 = spark.read.format("csv").option("header", "true").load("/data/emp_name.csv")
    val df2 = spark.read.format("csv").option("header", "true").load("/data/emp_addr.csv")

    val df3 = df2.filter( x => x.getString(1) == "home").withColumnRenamed("address_line", "home_address_line").withColumnRenamed("city", "home_city").drop("address_type")
    val df4 = df2.filter( x => x.getString(1) == "company").withColumnRenamed("address_line", "company_address_line").withColumnRenamed("city", "company_city").drop("address_type")

    val df5 = df1.join(df3, ("emp_id")).join(df4, ("emp_id"))

df1.show
df2.show
df5.show

	}
}
