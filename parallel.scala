import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import java.util.concurrent.Executors
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import org.apache.spark.sql.functions._
import org.apache.commons.text.StringSubstitutor;
import java.util._
import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.JavaConversions.mapAsJavaMap
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.DataFrame
import java.util.Calendar


object Main  extends App {

val now = Calendar.getInstance().getTime()
println(now)

val conf = new SparkConf().setAppName("Ontosides")
val spark = SparkSession.builder.config(conf).getOrCreate()

val construct_template1 =  """${question} ~ http://www.side-sante.fr/sides#has_for_number_of_proposals ~ ${np} ~ rule1"""
val construct_query1 =  """
SELECT s_1_2_t0.s AS question, string(count(s_1_2_t0.o)) AS np
FROM rdf_quad as s_1_2_t0 
WHERE s_1_2_t0.p = 'http://www.side-sante.fr/sides#has_for_proposal_of_answer'
GROUP BY s_1_2_t0.s
"""

val construct_template2=  """${answer} ~ http://www.side-sante.fr/sides#has_for_number_of_wrong_tick ~ ${nw} ~ rule2"""
val construct_query2 =  """
SELECT s_1_4_t0.o AS answer, COUNT ( s_1_4_t0.s) AS nw
 FROM rdf_quad AS s_1_4_t0
   INNER JOIN rdf_quad AS s_1_4_t1
   ON (
     s_1_4_t0.s = s_1_4_t1.s)
 WHERE
   s_1_4_t0.p = 'http://www.side-sante.fr/sides#is_part_of'
   AND
   s_1_4_t1.p = 'http://www.side-sante.fr/sides#has_wrongly_ticked'
 GROUP BY s_1_4_t0.o
"""

val construct_template3 =  """${answer} ~ http://www.side-sante.fr/sides#has_for_number_of_missed_right_tick ~ ${nm} ~ rule3"""
val construct_query3 =  """
SELECT s_1_12_t2.s AS answer, COUNT (s_1_12_t3.o) AS nm
  FROM rdf_quad AS s_1_12_t2
    INNER JOIN rdf_quad AS s_1_12_t3
    ON (
      s_1_12_t2.o = s_1_12_t3.s)
    INNER JOIN rdf_quad AS s_1_12_t4
    ON (
      s_1_12_t3.o = s_1_12_t4.s)
  WHERE
    s_1_12_t2.p = 'http://www.side-sante.fr/sides#correspond_to_question'
    AND
    s_1_12_t3.p = 'http://www.side-sante.fr/sides#has_for_proposal_of_answer'
    AND
    s_1_12_t4.p = 'http://www.side-sante.fr/sides#has_for_correction'
    AND
    s_1_12_t4.o = 1
    AND
    not  EXISTS ( 
       SELECT 1
        FROM rdf_quad AS s_1_10_t0
          INNER JOIN rdf_quad AS s_1_10_t1
          ON (
            s_1_10_t0.s = s_1_10_t1.s)
        WHERE
          s_1_10_t0.p = 'http://www.side-sante.fr/sides#is_part_of'
          AND
          s_1_10_t1.p = 'http://www.side-sante.fr/sides#has_rightly_ticked'
          AND
          s_1_10_t0.o = s_1_12_t2.s
          AND
          s_1_10_t1.o = s_1_12_t4.s
       )
  GROUP BY s_1_12_t2.s
"""

val construct_template4 =  """${answer} ~ http://www.side-sante.fr/sides#has_for_number_of_discordance ~ 0 ~ rule4"""
val construct_query4 =  """
SELECT s_1_10_t2.s AS answer
FROM rdf_quad AS s_1_10_t2
WHERE
  s_1_10_t2.p = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
  AND
  s_1_10_t2.o = 'http://www.side-sante.fr/sides#answer'
  AND
  not EXISTS ( 
     SELECT 1
      FROM rdf_quad AS s_1_4_t0
      WHERE
        s_1_4_t0.p = 'http://www.side-sante.fr/sides#has_for_number_of_wrong_tick'
        AND
        s_1_4_t0.s = s_1_10_t2.s
     )
  AND
  not EXISTS  (
     SELECT 1 
      FROM rdf_quad AS s_1_8_t1
      WHERE
        s_1_8_t1.p = 'http://www.side-sante.fr/sides#has_for_number_of_missed_right_tick'
        AND
        s_1_8_t1.s = s_1_10_t2.s
     )
"""

val construct_template5 =  """${answer} ~ http://www.side-sante.fr/sides#has_for_number_of_discordance ~ ${count} ~ rule5"""
val construct_query5 =  """
SELECT s_1_4_t0.s AS answer, ( s_1_4_t0.o +  s_1_4_t1.o) AS count
FROM rdf_quad AS s_1_4_t0
  INNER JOIN rdf_quad AS s_1_4_t1
  ON (
    s_1_4_t0.s = s_1_4_t1.s)
WHERE
  s_1_4_t0.p = 'http://www.side-sante.fr/sides#has_for_number_of_wrong_tick'
  AND
  s_1_4_t1.p = 'http://www.side-sante.fr/sides#has_for_number_of_missed_right_tick'
"""

val construct_template6 =  """${answer} ~ http://www.side-sante.fr/sides#has_for_number_of_discordance ~ ${nw} ~ rule6"""
val construct_query6 =  """
SELECT s_1_6_t1.s AS answer, s_1_6_t1.o AS nw
FROM rdf_quad AS s_1_6_t1
WHERE
  s_1_6_t1.p = 'http://www.side-sante.fr/sides#has_for_number_of_wrong_tick'
  AND
  not  EXISTS  (
     SELECT 1
      FROM rdf_quad AS s_1_4_t0
      WHERE
        s_1_4_t0.p = 'http://www.side-sante.fr/sides#has_for_number_of_missed_right_tick'
        AND
        s_1_4_t0.s = s_1_6_t1.s

     )
"""

val construct_template7 =  """${answer} ~ http://www.side-sante.fr/sides#has_for_number_of_discordance ~ ${nm} ~ rule7"""
val construct_query7 =  """
SELECT s_1_6_t1.s AS answer,
  s_1_6_t1.o AS nm
FROM rdf_quad AS s_1_6_t1
WHERE
  s_1_6_t1.p = 'http://www.side-sante.fr/sides#has_for_number_of_missed_right_tick'
  AND
  not  EXISTS (
     SELECT 1
      FROM rdf_quad AS s_1_4_t0
      WHERE
        s_1_4_t0.p = 'http://www.side-sante.fr/sides#has_for_number_of_wrong_tick'
        AND
        s_1_4_t0.s = s_1_6_t1.s

     )

"""

val construct_template8 =  """${answer} ~ http://www.side-sante.fr/sides#has_for_result ~ 1 ~ rule8"""
val construct_query8 =  """
SELECT s_1_2_t0.s AS answer
FROM rdf_quad AS s_1_2_t0
WHERE
  s_1_2_t0.p = 'http://www.side-sante.fr/sides#has_for_number_of_discordance'
  AND
  s_1_2_t0.o = 0
"""

val construct_template9 =  """${answer} ~ http://www.side-sante.fr/sides#has_for_result ~ 0  ~ rule9 | ${answer} ~ http://www.side-sante.fr/sides#stronglyWrong ~ true ~ rule9"""
val construct_query9 =  """ SELECT s_1_6_t0.o AS answer FROM rdf_quad AS s_1_6_t0 INNER JOIN rdf_quad AS s_1_6_t1 ON ( s_1_6_t0.s = s_1_6_t1.s) INNER JOIN rdf_quad AS s_1_6_t2 ON ( s_1_6_t1.o = s_1_6_t2.s) WHERE s_1_6_t0.p = 'http://www.side-sante.fr/sides#is_part_of' AND s_1_6_t1.p = 'http://www.side-sante.fr/sides#has_wrongly_ticked' AND s_1_6_t2.p = 'http://www.side-sante.fr/sides#has_for_weight_of_correction' AND s_1_6_t2.o = 'Unacceptable' """

val construct_template10 =  """${answer} ~ http://www.side-sante.fr/sides#has_for_result ~ 0  ~ rule10 | ${answer} ~ http://www.side-sante.fr/sides#stronglyWrong ~ true  ~ rule10"""
val construct_query10 =  """
SELECT s_1_14_t2.s AS answer
FROM rdf_quad AS s_1_14_t2
  INNER JOIN rdf_quad AS s_1_14_t3
  ON (
    s_1_14_t2.o = s_1_14_t3.s)
  INNER JOIN rdf_quad AS s_1_14_t4
  ON (
    s_1_14_t3.o = s_1_14_t4.s)
  INNER JOIN rdf_quad AS s_1_14_t5
  ON (
    s_1_14_t3.o = s_1_14_t5.s
    AND
    s_1_14_t4.s = s_1_14_t5.s)
WHERE
  s_1_14_t2.p = 'http://www.side-sante.fr/sides#correspond_to_question'
  AND
  s_1_14_t3.p = 'http://www.side-sante.fr/sides#has_for_proposal_of_answer'
  AND
  s_1_14_t4.p = 'http://www.side-sante.fr/sides#has_for_correction'
  AND
  s_1_14_t4.o = '1'
  AND
  s_1_14_t5.p = 'http://www.side-sante.fr/sides#has_for_weight_of_correction'
  AND
  s_1_14_t5.o = 'Indispensable'
  AND
  not  EXISTS ( 
     SELECT 1 
      FROM rdf_quad AS s_1_12_t0
        INNER JOIN rdf_quad AS s_1_12_t1
        ON (
          s_1_12_t0.s = s_1_12_t1.s)
      WHERE
        s_1_12_t0.p = 'http://www.side-sante.fr/sides#is_part_of'
        AND
        s_1_12_t1.p = 'http://www.side-sante.fr/sides#has_rightly_ticked'
        AND
        s_1_12_t0.o = s_1_14_t2.s
        AND
        s_1_12_t1.o = s_1_14_t5.s
     )
"""

val construct_template11 =  """${answer} ~ http://www.side-sante.fr/sides#has_for_result ~ 0 ~ rule11 | ${answer} ~ http://www.side-sante.fr/sides#stronglyWrong ~ true ~ rule11 """
val construct_query11 =  """
SELECT  s_1_6_t0.s AS answer
FROM rdf_quad AS s_1_6_t0
  INNER JOIN rdf_quad AS s_1_6_t1
  ON (
    s_1_6_t0.o = s_1_6_t1.s)
  INNER JOIN rdf_quad AS s_1_6_t2
  ON (
    s_1_6_t0.s = s_1_6_t2.s)
WHERE
  s_1_6_t0.p = 'http://www.side-sante.fr/sides#correspond_to_question'
  AND
  s_1_6_t1.p = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
  AND
  s_1_6_t1.o = 'http://www.side-sante.fr/sides#QUA'
  AND
  s_1_6_t2.p = 'http://www.side-sante.fr/sides#has_for_number_of_discordance'
  AND
 ( s_1_6_t2.o >  0)
"""

val construct_template12 =  """${answer} ~ http://www.side-sante.fr/sides#has_for_result ~ 0.5 ~ rule12"""
val construct_query12 =  """
SELECT s_1_10_t1.s AS answer
FROM rdf_quad AS s_1_10_t1
  INNER JOIN rdf_quad AS s_1_10_t2
  ON (
    s_1_10_t1.s = s_1_10_t2.s)
  INNER JOIN rdf_quad AS s_1_10_t3
  ON (
    s_1_10_t2.o = s_1_10_t3.s)
WHERE
  s_1_10_t1.p = 'http://www.side-sante.fr/sides#has_for_number_of_discordance'
  AND
  s_1_10_t1.o = 1
  AND
  s_1_10_t2.p = 'http://www.side-sante.fr/sides#correspond_to_question'
  AND
  s_1_10_t3.p = 'http://www.side-sante.fr/sides#has_for_number_of_proposals'
  AND
  s_1_10_t3.o = 5.
  AND
  not  EXISTS  (
     SELECT 1 
      FROM rdf_quad AS s_1_8_t0
      WHERE
        s_1_8_t0.p = 'http://www.side-sante.fr/sides#stronglyWrong'
        AND
        s_1_8_t0.o = 'true'
        AND
        s_1_8_t0.s = s_1_10_t2.s

     )
"""

val construct_template13 =  """${answer} ~ http://www.side-sante.fr/sides#has_for_result ~ 0.2 ~ rule13"""
val construct_query13 =  """
SELECT s_1_10_t1.s AS answer
FROM rdf_quad AS s_1_10_t1
  INNER JOIN rdf_quad AS s_1_10_t2
  ON (
    s_1_10_t1.s = s_1_10_t2.s)
  INNER JOIN rdf_quad AS s_1_10_t3
  ON (
    s_1_10_t2.o = s_1_10_t3.s)
WHERE
  s_1_10_t1.p = 'http://www.side-sante.fr/sides#has_for_number_of_discordance'
  AND
  s_1_10_t1.o = 2
  AND
  s_1_10_t2.p = 'http://www.side-sante.fr/sides#correspond_to_question'
  AND
  s_1_10_t3.p = 'http://www.side-sante.fr/sides#has_for_number_of_proposals'
  AND
  s_1_10_t3.o = 5
  AND
  not  EXISTS (
     SELECT 1
      FROM rdf_quad AS s_1_8_t0
      WHERE
        s_1_8_t0.p = 'http://www.side-sante.fr/sides#stronglyWrong'
        AND
        s_1_8_t0.o = 'true'
        AND
        s_1_8_t0.s = s_1_10_t2.s

     )
"""


val construct_template14 =  """${answer} ~ http://www.side-sante.fr/sides#has_for_result ~ 0.425 ~ rule14"""
val construct_query14 =  """
SELECT s_1_10_t1.s AS answer
FROM rdf_quad AS s_1_10_t1
  INNER JOIN rdf_quad AS s_1_10_t2
  ON (
    s_1_10_t1.s = s_1_10_t2.s)
  INNER JOIN rdf_quad AS s_1_10_t3
  ON (
    s_1_10_t2.o = s_1_10_t3.s)
WHERE
  s_1_10_t1.p = 'http://www.side-sante.fr/sides#has_for_number_of_discordance'
  AND
  s_1_10_t1.o = 1
  AND
  s_1_10_t2.p = 'http://www.side-sante.fr/sides#correspond_to_question'
  AND
  s_1_10_t3.p = 'http://www.side-sante.fr/sides#has_for_number_of_proposals'
  AND
  s_1_10_t3.o = 4
  AND
  not  EXISTS ( 
     SELECT 1 
      FROM rdf_quad AS s_1_8_t0
      WHERE
        s_1_8_t0.p = 'http://www.side-sante.fr/sides#stronglyWrong'
        AND
        s_1_8_t0.o = 'true'
        AND
        s_1_8_t0.s = s_1_10_t2.s

     )

"""

val construct_template15 =  """${answer} ~ http://www.side-sante.fr/sides#has_for_result ~ 0.1 ~ rule15"""
val construct_query15 =  """
SELECT s_1_10_t1.s AS answer
FROM rdf_quad AS s_1_10_t1
  INNER JOIN rdf_quad AS s_1_10_t2
  ON (
    s_1_10_t1.s = s_1_10_t2.s)
  INNER JOIN rdf_quad AS s_1_10_t3
  ON (
    s_1_10_t2.o = s_1_10_t3.s)
WHERE
  s_1_10_t1.p = 'http://www.side-sante.fr/sides#has_for_number_of_discordance'
  AND
  s_1_10_t1.o = 2
  AND
  s_1_10_t2.p = 'http://www.side-sante.fr/sides#correspond_to_question'
  AND
  s_1_10_t3.p = 'http://www.side-sante.fr/sides#has_for_number_of_proposals'
  AND
  s_1_10_t3.o = 4
  AND
  not EXISTS (
     SELECT 1
      FROM rdf_quad AS s_1_8_t0
      WHERE
        s_1_8_t0.p = 'http://www.side-sante.fr/sides#stronglyWrong'
        AND
        s_1_8_t0.o = 'true'
        AND
        s_1_8_t0.s = s_1_10_t2.s

     )
"""

val construct_template16 =  """${answer} ~ http://www.side-sante.fr/sides#has_for_result ~ 0 ~ rule16"""
val construct_query16 =  """
SELECT s_1_6_t0.s AS answer
FROM rdf_quad AS s_1_6_t0
  INNER JOIN rdf_quad AS s_1_6_t1
  ON (
    s_1_6_t0.o = s_1_6_t1.s)
  INNER JOIN rdf_quad AS s_1_6_t2
  ON (
    s_1_6_t0.s = s_1_6_t2.s)
WHERE
  s_1_6_t0.p = 'http://www.side-sante.fr/sides#correspond_to_question'
  AND
  s_1_6_t1.p = 'http://www.side-sante.fr/sides#has_for_number_of_proposals'
  AND
  s_1_6_t2.p = 'http://www.side-sante.fr/sides#has_for_number_of_discordance'
  AND
 ( s_1_6_t1.o >  3)
  AND
 ( s_1_6_t1.o <  6)
  AND
 ( s_1_6_t2.o >  2)
"""

val construct_template17 =  """${answer} ~ http://www.side-sante.fr/sides#has_for_result ~ 0.3 ~ rule17"""
val construct_query17 =  """
SELECT s_1_10_t1.s AS answer
FROM rdf_quad AS s_1_10_t1
  INNER JOIN rdf_quad AS s_1_10_t2
  ON (
    s_1_10_t1.s = s_1_10_t2.s)
  INNER JOIN rdf_quad AS s_1_10_t3
  ON (
    s_1_10_t2.o = s_1_10_t3.s)
WHERE
  s_1_10_t1.p = 'http://www.side-sante.fr/sides#has_for_number_of_discordance'
  AND
  s_1_10_t1.o = 1
  AND
  s_1_10_t2.p = 'http://www.side-sante.fr/sides#correspond_to_question'
  AND
  s_1_10_t3.p = 'http://www.side-sante.fr/sides#has_for_number_of_proposals'
  AND
  s_1_10_t3.o = 3
  AND
  not EXISTS (
     SELECT 1
      FROM rdf_quad AS s_1_8_t0
      WHERE
        s_1_8_t0.p = 'http://www.side-sante.fr/sides#stronglyWrong'
        AND
        s_1_8_t0.o = 'true'
        AND
        s_1_8_t0.s = s_1_10_t2.s

     )
"""

val construct_template18 =  """${answer} ~ http://www.side-sante.fr/sides#has_for_result ~ 0 ~ rule18"""
val construct_query18 =  """
SELECT s_1_6_t0.s AS answer
FROM rdf_quad AS s_1_6_t0
  INNER JOIN rdf_quad AS s_1_6_t1
  ON (
    s_1_6_t0.s = s_1_6_t1.s)
  INNER JOIN rdf_quad AS s_1_6_t2
  ON (
    s_1_6_t1.o = s_1_6_t2.s)
WHERE
  s_1_6_t0.p = 'http://www.side-sante.fr/sides#has_for_number_of_discordance'
  AND
  s_1_6_t1.p = 'http://www.side-sante.fr/sides#correspond_to_question'
  AND
  s_1_6_t2.p = 'http://www.side-sante.fr/sides#has_for_number_of_proposals'
  AND
  s_1_6_t2.o = 3.
  AND
 ( s_1_6_t0.o >  1)
"""

object Reasoner {
  def rule(construct_query:String, construct_template: String, df: DataFrame) : DataFrame = {

      val construct = udf((row: scala.collection.immutable.Map[String, String]) => {
      val templateString: String = construct_template 
      val sub: StringSubstitutor = new StringSubstitutor(mapAsJavaMap(row));
      val resolvedString: String = sub.replace(templateString);
      val retrunStr = resolvedString
        retrunStr
      })

	  df.createOrReplaceTempView("rdf_quad")

	  val r1 = spark.sql(construct_query)

	  val colnms_n_vals = r1.columns.flatMap { c => Array(lit(c), col(c)) }

	  val df1 = r1.withColumn("b",  map(colnms_n_vals:_*))

	  val df2 = df1.withColumn("c", construct(col("b")))

	  val df3 = df2.withColumn("c", explode(split(col("c"), "\\|"))).select(col("c"))

	  val df4 = df3.withColumn("_tmp", split(col("c"), "~")).select(
		col("_tmp").getItem(0).as("s"),
		col("_tmp").getItem(1).as("p"),
		col("_tmp").getItem(2).as("o"),
		col("_tmp").getItem(3).as("g")
	  )

	  //df4.show(20, false)

	  val df5 = df4.withColumn("s",trim(col("s"))).withColumn("p",trim(col("p"))).withColumn("o",trim(col("o"))).withColumn("g",trim(col("g")))

	  return df5
  }
}

val df = spark.read.format("delta").load("hdfs://hadoop-namenode/node1")
df.printSchema()
df.show(10, false)
df.count()

//Allowing a maximum of 6 threads to run
val executorService = Executors.newFixedThreadPool(6)
implicit val executionContext = ExecutionContext.fromExecutorService(executorService)

// layer 1

val fq10 = Future {
    println("~~~rule 10~~~")
    val df10 = Reasoner.rule(construct_query10, construct_template10, df)
    val df101 = df10.dropDuplicates()
    spark.time(df101.write.format("delta").mode("overwrite").save("hdfs://hadoop-namenode/rule10"))
}

val fq1 = Future {
    println("~~~rule 1~~~")
    val df1 = Reasoner.rule(construct_query1, construct_template1, df)
    spark.time(df1.write.format("delta").mode("overwrite").save("hdfs://hadoop-namenode/rule1"))
}

val fq2 = Future {
    println("~~~rule 2~~~")
    val df2 = Reasoner.rule(construct_query2, construct_template2, df)
    spark.time(df2.write.format("delta").mode("overwrite").save("hdfs://hadoop-namenode/rule2"))
}

val fq3 = Future {
    println("~~~rule 3~~~")
    val df3 = Reasoner.rule(construct_query3, construct_template3, df)
    spark.time(df3.write.format("delta").mode("overwrite").save("hdfs://hadoop-namenode/rule3"))
}

val fq9 = Future {
    println("~~~rule 9~~~")
    val df9 = Reasoner.rule(construct_query9, construct_template9, df)
    val df91 = df9.dropDuplicates()
    spark.time(df91.write.format("delta").mode("overwrite").save("hdfs://hadoop-namenode/rule9"))
}


  Await.result(fq10, Duration.Inf)
  Await.result(fq1, Duration.Inf)
  Await.result(fq2, Duration.Inf)
  Await.result(fq3, Duration.Inf)
  Await.result(fq9, Duration.Inf)

val rule1 = spark.read.format("delta").load("hdfs://hadoop-namenode/rule1")
val rule2 = spark.read.format("delta").load("hdfs://hadoop-namenode/rule2")
val rule3 = spark.read.format("delta").load("hdfs://hadoop-namenode/rule3")
val rule9 = spark.read.format("delta").load("hdfs://hadoop-namenode/rule9")
val rule10 = spark.read.format("delta").load("hdfs://hadoop-namenode/rule10")

rule1.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1")
rule2.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1")
rule3.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1")
rule9.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1")
rule10.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1")


// layer 2


val fq4 = Future {
    println("~~~rule 4~~~")
    val df4 = Reasoner.rule(construct_query4, construct_template4, df)
    spark.time(df4.write.format("delta").mode("overwrite").save("hdfs://hadoop-namenode/rule4"))
}

val fq5 = Future {
    println("~~~rule 5~~~")
    val df5 = Reasoner.rule(construct_query5, construct_template5, df)
    spark.time(df5.write.format("delta").mode("overwrite").save("hdfs://hadoop-namenode/rule5"))
}

val fq6 = Future {
    println("~~~rule 6~~~")
    val df6 = Reasoner.rule(construct_query6, construct_template6, df)
    spark.time(df6.write.format("delta").mode("overwrite").save("hdfs://hadoop-namenode/rule6"))
}

val fq7 = Future {
    println("~~~rule 7~~~")
    val df7 = Reasoner.rule(construct_query7, construct_template7, df)
    spark.time(df7.write.format("delta").mode("overwrite").save("hdfs://hadoop-namenode/rule7"))
}


  Await.result(fq4, Duration.Inf)
  Await.result(fq5, Duration.Inf)
  Await.result(fq6, Duration.Inf)
  Await.result(fq7, Duration.Inf)

val rule4 = spark.read.format("delta").load("hdfs://hadoop-namenode/rule4")
val rule5 = spark.read.format("delta").load("hdfs://hadoop-namenode/rule5")
val rule6 = spark.read.format("delta").load("hdfs://hadoop-namenode/rule6")
val rule7 = spark.read.format("delta").load("hdfs://hadoop-namenode/rule7")

rule4.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1")
rule5.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1")
rule6.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1")
rule7.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1")

// layer 3


val fq8 = Future {
    println("~~~rule 8~~~")
    val df8 = Reasoner.rule(construct_query8, construct_template8, df)
    spark.time(df8.write.format("delta").mode("overwrite").save("hdfs://hadoop-namenode/rule8"))
}

val fq12 = Future {
    println("~~~rule 12~~~")
    val df12 = Reasoner.rule(construct_query12, construct_template12, df)
    spark.time(df12.write.format("delta").mode("overwrite").save("hdfs://hadoop-namenode/rule12"))
}

val fq11 = Future {
    println("~~~rule 11~~~")
    val df11 = Reasoner.rule(construct_query11, construct_template11, df)
    val df111 = df11.dropDuplicates()
    spark.time(df111.write.format("delta").mode("overwrite").save("hdfs://hadoop-namenode/rule11"))
}


  Await.result(fq8, Duration.Inf)
  Await.result(fq12, Duration.Inf)
  Await.result(fq11, Duration.Inf)

val rule8 = spark.read.format("delta").load("hdfs://hadoop-namenode/rule8")
val rule12 = spark.read.format("delta").load("hdfs://hadoop-namenode/rule12")
val rule11 = spark.read.format("delta").load("hdfs://hadoop-namenode/rule11")

rule8.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1")
rule12.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1")
rule11.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1")


// layer 4

val fq13 = Future {
    println("~~~rule 13~~~")
    val df13 = Reasoner.rule(construct_query13, construct_template13, df)
    spark.time(df13.write.format("delta").mode("overwrite").save("hdfs://hadoop-namenode/rule13"))
}

val fq14 = Future {
    println("~~~rule 14~~~")
    val df14 = Reasoner.rule(construct_query14, construct_template14, df)
    spark.time(df14.write.format("delta").mode("overwrite").save("hdfs://hadoop-namenode/rule14"))
}

val fq15 = Future {
    println("~~~rule 15~~~")
    val df15 = Reasoner.rule(construct_query15, construct_template15, df)
    spark.time(df15.write.format("delta").mode("overwrite").save("hdfs://hadoop-namenode/rule15"))
}

val fq16 = Future {
    println("~~~rule 16~~~")
    val df16 = Reasoner.rule(construct_query16, construct_template16, df)
    spark.time(df16.write.format("delta").mode("overwrite").save("hdfs://hadoop-namenode/rule16"))
}

val fq17 = Future {
    println("~~~rule 17~~~")
    val df17 = Reasoner.rule(construct_query17, construct_template17, df)
    spark.time(df17.write.format("delta").mode("overwrite").save("hdfs://hadoop-namenode/rule17"))
}

val fq18 = Future {
    println("~~~rule 18~~~")
    val df18 = Reasoner.rule(construct_query18, construct_template18, df)
    spark.time(df18.write.format("delta").mode("overwrite").save("hdfs://hadoop-namenode/rule18"))
}

  Await.result(fq13, Duration.Inf)
  Await.result(fq14, Duration.Inf)
  Await.result(fq15, Duration.Inf)
  Await.result(fq16, Duration.Inf)
  Await.result(fq17, Duration.Inf)
  Await.result(fq18, Duration.Inf)

val rule13 = spark.read.format("delta").load("hdfs://hadoop-namenode/rule13")
val rule14 = spark.read.format("delta").load("hdfs://hadoop-namenode/rule14")
val rule15 = spark.read.format("delta").load("hdfs://hadoop-namenode/rule15")
val rule16 = spark.read.format("delta").load("hdfs://hadoop-namenode/rule16")
val rule17 = spark.read.format("delta").load("hdfs://hadoop-namenode/rule17")
val rule18 = spark.read.format("delta").load("hdfs://hadoop-namenode/rule18")

rule13.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1")
rule14.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1")
rule15.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1")
rule16.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1")
rule17.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1")
rule18.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1")


println(now)

}
