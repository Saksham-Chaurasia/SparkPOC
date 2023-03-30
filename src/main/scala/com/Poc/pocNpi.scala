package com.Poc

import com.utility.Cont.entity
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
object pocNpi {

  @transient lazy val logger:Logger =Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("falanadhamaka")
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cassandra.connection.port", "9042")
      .config("spark.sql.shuffle.paritions", 3)
      .config("stopGracefullyOnShutdown", true)
      .getOrCreate()


    // getting the main input data
    val npiData = spark.read
      .format("csv")
      .option("header", true)
      .option("inferschema", true)
      .option("dateFormat", "MM/dd/yyyy")
      .load("data2/npidata_pfile_20230306-20230312.csv")

    //changing the column names

    val npiDataColumn = npiData.select(Seq.empty[org.apache.spark.sql.Column] ++
      npiData.columns.map(colName => col("`" + colName + "`").as(colName.toLowerCase.replace(" ", "_")))
      : _*)

    // getting the deactivated people data
    val monthDeactivated = spark.read.format("csv")
      .option("header", true)
      .option("inferschema", true)
      .option("dateFormat", "MM/dd/yyyy")
      .load("data2/NPPES_Deactivated_NPI_Report_20230313.csv")

    // changing the column
    val monthDeactivatedColumn = monthDeactivated.select(Seq.empty[org.apache.spark.sql.Column] ++
      monthDeactivated.columns.map(colName => col(colName).as(colName.toLowerCase.replace(" ", "_"))): _*)

    // to change the column name we can use the command
    val monthDeactivatedColumn2 = monthDeactivatedColumn.withColumnRenamed("npi", "id")

    // creating a temporary view
    npiDataColumn.createOrReplaceTempView("npi")
    monthDeactivatedColumn2.createOrReplaceTempView("deactivate")

    // extracting the activated and non -activated people
    val activated = spark.sql("select * from npi Left join deactivate on npi.npi == deactivate.id where deactivate.id is null ")
    val deactivated = spark.sql("select * from npi join deactivate on npi.npi == deactivate.id ")

    activated.show()
    deactivated.show()
    // mapping the particular column
    val activatednew = activated.withColumn("entity_type_code",entity(col("entity_type_code")))
    val deactivatednew = deactivated.withColumn("entity_type_code",entity(col("entity_type_code")))

    deactivatednew.show()





    //now writing this data to cassandra
    // converting the datatype of the npi,

    val uactivated= activatednew.withColumn("npi", col("npi").cast(StringType))
    uactivated.printSchema()
    val udeactivated = deactivatednew.withColumn("id", col("id").cast(StringType))

    // filtering the particular columns

    val uactivated2 = uactivated.select(col("npi"), col("entity_type_code"),
      col("provider_first_name"), col("provider_middle_name"), col("provider_enumeration_date"),
      col("npi_deactivation_date"), col("authorized_official_title_or_position"), col("authorized_official_telephone_number"))
    uactivated2.printSchema()
    uactivated2.show()

    val udeactivated2 = udeactivated.select(col("id"), col("entity_type_code"),
      col("provider_first_name"), col("provider_middle_name"), col("provider_enumeration_date"),
      col("npi_deactivation_date"), col("authorized_official_title_or_position"), col("authorized_official_telephone_number"))
    udeactivated2.printSchema()
    udeactivated2.show()

    uactivated2.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "activate", "keyspace" -> "sparkpoc"))
      .mode("append")
      .save()

    udeactivated2.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "deactivate", "keyspace" -> "sparkpoc"))
      .mode("append") // overwrite, ignore, error
      .save()

    //giving to kafka

    val convertActivated = uactivated2.selectExpr("cast(npi.npi as String) as  key",
      """to_json(struct(*))as value""".stripMargin)

    val convertDeactivated = udeactivated2.selectExpr("cast(deactivate.id as string)as key",
      """to_json(struct(*))as value""".stripMargin)

    val writeActivated = convertActivated.write
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "activated")
      .save()

    val writeDeactivated = convertDeactivated.write
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "deactivated")
      .save()
    // upto here kafka


    // Convert the input DataFrame to a stream by adding a timestamp column
    // writing into kafka in a stream way
    /*
       val streamDF = activated.withColumn("timestamp", current_timestamp())

       val kafkaWriter = streamDF
         .selectExpr("to_json(struct(*)) as value")
         .writeStream
         .trigger(Trigger.ProcessingTime("10 seconds"))
         .outputMode("append")
         .format("kafka")
         .option("topic", "npistrem")
         .option("checkpointLocation","chk-point-dir-stream")
         .start()

        */

  }
/*
  create table deactivate (id text primary key, entity_type_code text, provider_first_name text,
    provider_middle_name text,provider_enumeration_date text,npi_deactivation_date text,
    authorized_official_title_or_position text,authorized_official_telephone_number bigInt);


create table activate (npi text primary key, entity_type_code text, provider_first_name text,
    provider_middle_name text,provider_enumeration_date text,npi_deactivation_date text,
    authorized_official_title_or_position text,authorized_official_telephone_number bigInt);


 */

}


