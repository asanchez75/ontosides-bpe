import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.commons.text.StringSubstitutor;
import java.util._
import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.JavaConversions.mapAsJavaMap
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.DataFrame
import java.util.Calendar

val now = Calendar.getInstance().getTime()

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

	  val df5 = df4.withColumn("s",trim(col("s"))).withColumn("p",trim(col("p"))).withColumn("o",trim(col("o"))).withColumn("g",trim(col("g")))

	  return df5
  }
}


val df = spark.read.format("delta").load("hdfs://hadoop-namenode/node1")
df.printSchema()
df.show(10, false)
df.count()

///////////////////////////////////////////////////////////////////////////////

val construct_template = construct_template1 
val construct_query =  construct_query1

val df1 = Reasoner.rule(construct_query, construct_template, df)
spark.time(df1.collect.foreach(println))
//df1.show(20, false)

println("~~~rule 1~~~")
spark.time(df1.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1"))
println("~~~rule1 count: " + df1.count)
println("~~~main count: " + df.count)

/////////////////////////////////////////////////////////////////////////////////

val construct_template = construct_template2 
val construct_query =  construct_query2

val df2 = Reasoner.rule(construct_query, construct_template, df)
spark.time(df2.collect.foreach(println))
//df2.show(20, false)

println("~~~rule 2~~~")
spark.time(df2.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1"))
println("~~~rule2 count: " + df2.count)
println("~~~main count: " + df.count)

/////////////////////////////////////////////////////////////////////////////////

val construct_template = construct_template3 
val construct_query =  construct_query3

val df3 = Reasoner.rule(construct_query, construct_template, df)
spark.time(df3.collect.foreach(println))
//df3.show(20, false)

println("~~~rule 3~~~")
spark.time(df3.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1"))
println("~~~rule3 count: " + df3.count)
println("~~~main count: " + df.count)

/////////////////////////////////////////////////////////////////////////////////

val construct_template = construct_template4 
val construct_query =  construct_query4

val df4 = Reasoner.rule(construct_query, construct_template, df)
spark.time(df4.collect.foreach(println))
//df4.show(20, false)

println("~~~rule r4~~~")
spark.time(df4.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1"))
println("~~~rule4 count: " + df4.count)
println("~~~main count: " + df.count)

/////////////////////////////////////////////////////////////////////////////////

val construct_template = construct_template5 
val construct_query =  construct_query5

val df5 = Reasoner.rule(construct_query, construct_template, df)
spark.time(df5.collect.foreach(println))
//df5.show(20, false)

println("~~~rule r5~~~")
spark.time(df5.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1"))
println("~~~rule5 count: " + df5.count)
println("~~~main count: " + df.count)

/////////////////////////////////////////////////////////////////////////////////

val construct_template = construct_template6 
val construct_query =  construct_query6

val df6 = Reasoner.rule(construct_query, construct_template, df)
spark.time(df6.collect.foreach(println))
//df6.show(20, false)
println("~~~rule 6~~~")
spark.time(df6.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1"))
println("~~~rule6 count: " + df6.count)
println("~~~main count: " + df.count)

/////////////////////////////////////////////////////////////////////////////////

val construct_template = construct_template7 
val construct_query =  construct_query7

val df7 = Reasoner.rule(construct_query, construct_template, df)
spark.time(df7.collect.foreach(println))
//df7.show(20, false)

println("~~~rule 7~~~")
spark.time(df7.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1"))
println("~~~rule7 count: " + df7.count)
println("~~~main count: " + df.count)


/////////////////////////////////////////////////////////////////////////////////

val construct_template = construct_template8 
val construct_query =  construct_query8

val df8 = Reasoner.rule(construct_query, construct_template, df)
spark.time(df8.collect.foreach(println))
//df8.show(20, false)

println("~~~rule 8~~~")
spark.time(df8.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1"))
println("~~~rule8 count: " + df8.count)
println("~~~main count: " + df.count)

/////////////////////////////////////////////////////////////////////////////////

val construct_template = construct_template9 
val construct_query =  construct_query9

val df9 = Reasoner.rule(construct_query, construct_template, df)
spark.time(df9.collect.foreach(println))
spark.time(df9.dropDuplicates())
val df91 = df9.dropDuplicates()
//df9.show(20, false)

println("~~~rule 9~~~")
spark.time(df91.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1"))
println("~~~rule9 count: " + df91.count)
println("~~~main count: " + df.count)

/////////////////////////////////////////////////////////////////////////////////

val construct_template = construct_template10
val construct_query =  construct_query10

val df10 = Reasoner.rule(construct_query, construct_template, df)
spark.time(df10.collect.foreach(println))
spark.time(df10.dropDuplicates())
val df101 = df10.dropDuplicates()
//df10.show(20, false)

println("~~~rule 10~~~")
spark.time(df101.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1"))
println("~~~rule10 count: " + df101.count)
println("~~~main count: " + df.count)

/////////////////////////////////////////////////////////////////////////////////

val construct_template = construct_template11
val construct_query =  construct_query11

val df11 = Reasoner.rule(construct_query, construct_template, df)
spark.time(df11.collect.foreach(println))
spark.time(df11.dropDuplicates())
val df111 = df11.dropDuplicates()
//df11.show(20, false)

println("~~~rule 11~~~")
spark.time(df111.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1"))
println("~~~rule11 count: " + df111.count)
println("~~~main count: " + df.count)


/////////////////////////////////////////////////////////////////////////////////

val construct_template = construct_template12
val construct_query =  construct_query12

val df12 = Reasoner.rule(construct_query, construct_template, df)
spark.time(df12.collect.foreach(println))

//df12.show(20, false)

println("~~~rule 12~~~")
spark.time(df12.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1"))
println("~~~rule12 count: " + df12.count)
println("~~~main count: " + df.count)


/////////////////////////////////////////////////////////////////////////////////

val construct_template = construct_template13
val construct_query =  construct_query13

val df13 = Reasoner.rule(construct_query, construct_template, df)
spark.time(df13.collect.foreach(println))
//df13.show(20, false)
println("~~~rule 13~~~")
spark.time(df13.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1"))
println("~~~rule13 count: " + df13.count)
println("~~~main count: " + df.count)

/////////////////////////////////////////////////////////////////////////////////

val construct_template = construct_template14
val construct_query =  construct_query14

val df14 = Reasoner.rule(construct_query, construct_template, df)
spark.time(df14.collect.foreach(println))
//df14.show(20, false)
println("~~~rule 14~~~")
spark.time(df14.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1"))
println("~~~rule14 count: " + df14.count)
println("~~~main count: " + df.count)

/////////////////////////////////////////////////////////////////////////////////

val construct_template = construct_template15
val construct_query =  construct_query15

val df15 = Reasoner.rule(construct_query, construct_template, df)
spark.time(df15.collect.foreach(println))
//df15.show(20, false)
println("~~~rule 15~~~")
spark.time(df15.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1"))
println("~~~rule15 count: " + df15.count)
println("~~~main count: " + df.count)

/////////////////////////////////////////////////////////////////////////////////

val construct_template = construct_template16
val construct_query =  construct_query16

val df16 = Reasoner.rule(construct_query, construct_template, df)
spark.time(df16.collect.foreach(println))
//df16.show(20, false)
println("~~~rule 16~~~")
spark.time(df16.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1"))
println("~~~rule16 count: " + df16.count)
println("~~~main count: " + df.count)

/////////////////////////////////////////////////////////////////////////////////

val construct_template = construct_template17
val construct_query =  construct_query17

val df17 = Reasoner.rule(construct_query, construct_template, df)
spark.time(df17.collect.foreach(println))
//df17.show(20, false)

println("~~~rule 17~~~")
spark.time(df17.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1"))
println("~~~rule17 count: " + df17.count)
println("~~~main count: " + df.count)

/////////////////////////////////////////////////////////////////////////////////

val construct_template = construct_template18
val construct_query =  construct_query18

val df18 = Reasoner.rule(construct_query, construct_template, df)
spark.time(df18.collect.foreach(println))
//df18.show(20, false)
println("~~~rule 18~~~")
spark.time(df18.write.format("delta").mode("append").save("hdfs://hadoop-namenode/node1"))
println("~~~rule18 count: " + df18.count)
println("~~~main count: " + df.count)

val now = Calendar.getInstance().getTime()
