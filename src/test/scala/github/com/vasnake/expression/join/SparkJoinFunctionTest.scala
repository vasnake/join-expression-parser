/*
 * Copyright (c) 2020-2021, vasnake@gmail.com
 *
 * Licensed under GNU General Public License v3.0 (see LICENSE)
 *
 */

package github.com.vasnake.expression.join

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

//import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql

import github.com.vasnake.spark.test.SimpleLocalSpark

class SparkJoinFunctionTest extends
  AnyFlatSpec with Matchers  with SimpleLocalSpark{

  import sql.DataFrame
  import Implicits._
  import SparkJoinFunctionTest._

  it should "return source after pseudo-join" in {
    import spark.implicits._

    val tables: Map[String, DataFrame] = Map(
      "foo" -> Seq(SourceRow("dt: 2020-05-05, uid: 42, uid_type: OKID, pFeature1: true, pFeature2: foo")).toDF
    )

    val rule = EtlFeatures.parseJoinRule("foo", "bar")

    val expected = Seq(
      "[2020-05-05,42,OKID,true,foo,null,null,null,null,null,null]"
    )

    val actual = EtlFeatures.joinWithAliases(tables, rule)
      .collect.map(_.toString)

    actual should contain theSameElementsAs expected
  }

  it should "join few sources" in {
    import spark.implicits._

    val tables: Map[String, DataFrame] = Map(
      "a" -> Seq(SourceRow("uid: 42, uid_type: OKID, pFeature1: true")).toDF.selectCSVcols( "uid,uid_type,pFeature1"),
      "b" -> Seq(SourceRow("uid: 42, uid_type: OKID, pFeature2: 2")).toDF.selectCSVcols(    "uid,uid_type,pFeature2"),
      "c" -> Seq(SourceRow("uid: 42, uid_type: OKID, pFeature3: 3")).toDF.selectCSVcols(    "uid,uid_type,pFeature3"),
      "d" -> Seq(SourceRow("uid: 42, uid_type: OKID, pFeature4: 4")).toDF.selectCSVcols(    "uid,uid_type,pFeature4")
    )

    val rule = EtlFeatures.parseJoinRule("a outer (b left_outer c) inner d", "")

    val expected: Seq[String] = Seq(
      "[42,OKID,true,2,3,4.0]"
    )

    val actual = EtlFeatures.joinWithAliases(tables, rule).collect.map(_.toString)

    actual should contain theSameElementsAs expected
  }

  it should "assign aliases to joined dfs" in {
    import spark.implicits._

    val tables: Map[String, DataFrame] = Map(
      "a" -> Seq(SourceRow("uid: 42, uid_type: OKID, pFeature1: true")).toDF.selectCSVcols( "uid,uid_type,pFeature1"),
      "b" -> Seq(SourceRow("uid: 42, uid_type: OKID, pFeature2: 2")).toDF.selectCSVcols(    "uid,uid_type,pFeature2"),
      "c" -> Seq(SourceRow("uid: 42, uid_type: OKID, pFeature3: 3")).toDF.selectCSVcols(    "uid,uid_type,pFeature3"),
      "d" -> Seq(SourceRow("uid: 42, uid_type: OKID, pFeature4: 4")).toDF.selectCSVcols(    "uid,uid_type,pFeature4")
    )

    val rule = EtlFeatures.parseJoinRule("a inner b inner c inner d", "")

    val df = EtlFeatures.joinWithAliases(tables, rule)
    def expr(e: String) = df.select(sql.functions.expr(e)).collect.map(_.toString)

    expr("a.*") should contain theSameElementsAs Seq("[42,OKID,true]")
    expr("b.*") should contain theSameElementsAs Seq("[2]")
    expr("c.*") should contain theSameElementsAs Seq("[3]")
    expr("d.*") should contain theSameElementsAs Seq("[4.0]")
  }

  // testOnly *JoinFunction* -- -z "different"
  it should "perform different joins" in {
    // inner, full_outer, left_outer, right_outer, left_semi, left_anti,
    // no `cross` while using columns (natural join)
    import spark.implicits._

    val tables: Map[String, DataFrame] = Map(
      "a" -> Seq(
        SourceRow("uid: 41, uid_type: OKID, pFeature1: true"),
        SourceRow("uid: 42, uid_type: OKID, pFeature1: true")
      ).toDF.selectCSVcols( "uid,uid_type,pFeature1"),
      "b" -> Seq(
        SourceRow("uid: 42, uid_type: OKID, pFeature2: 2"),
        SourceRow("uid: 43, uid_type: OKID, pFeature2: 2")
      ).toDF.selectCSVcols("uid,uid_type,pFeature2")
    )

    def join(rule: String): Seq[String] = EtlFeatures.joinWithAliases(tables, EtlFeatures.parseJoinRule(rule, "")).collect.map(_.toString).toSeq

    join("a left_anti b") should contain theSameElementsAs Seq("[41,OKID,true]")
    join("a left_semi b") should contain theSameElementsAs Seq("[42,OKID,true]")
    join("a right_outer b") should contain theSameElementsAs Seq("[42,OKID,true,2]", "[43,OKID,null,2]")
    join("a left_outer b") should contain theSameElementsAs Seq("[41,OKID,true,null]", "[42,OKID,true,2]")
    join("a full_outer b") should contain theSameElementsAs Seq("[41,OKID,true,null]", "[42,OKID,true,2]", "[43,OKID,null,2]")
    join("a inner b") should contain theSameElementsAs Seq("[42,OKID,true,2]")
  }

  // testOnly *JoinFunction* -- -z "order"
  it should "perform join in given order" in {
    import spark.implicits._

    val tables: Map[String, DataFrame] = Map(
      "a" -> Seq(
        SourceRow("uid: 41, uid_type: OKID, pFeature1: true"),
        SourceRow("uid: 42, uid_type: OKID, pFeature1: true")
      ).toDF.selectCSVcols("uid,uid_type,pFeature1"),

      "b" -> Seq(SourceRow("uid: 42, uid_type: OKID, pFeature2: 2")).toDF.selectCSVcols("uid,uid_type,pFeature2"),
      "c" -> Seq(SourceRow("uid: 42, uid_type: OKID, pFeature3: 3")).toDF.selectCSVcols("uid,uid_type,pFeature3"),

      "d" -> Seq(
        SourceRow("uid: 41, uid_type: OKID, pFeature4: 4"),
        SourceRow("uid: 42, uid_type: OKID, pFeature4: 4")
      ).toDF.selectCSVcols("uid,uid_type,pFeature4")
    )

    val rule = EtlFeatures.parseJoinRule("(a left_outer b) inner (c right_outer d)", "")

    val expected: Seq[String] = Seq(
      "[41,OKID,true,null,null,4.0]",
      "[42,OKID,true,2,3,4.0]"
    )

    val actual = EtlFeatures.joinWithAliases(tables, rule).collect.map(_.toString)

    actual should contain theSameElementsAs expected
  }
}

object SparkJoinFunctionTest {

  case class SourceRow
  (
    dt: Option[String] = None,
    uid: Option[Int] = None,
    uid_type: Option[String] = None,
    pFeature1: Option[Boolean] = None,
    pFeature2: Option[String] = None,
    pFeature3: Option[Int] = None,
    pFeature4: Option[Float] = None,
    pFeature5: Option[Double] = None,
    mFeature: Option[Map[String, Int]] = None,
    aFeature: Option[Array[Option[Int]]] = None,
    id: Option[Int] = None
  )

  object SourceRow {
    def apply(data: String): SourceRow = {
      import github.com.vasnake.toolbox.StringToolbox._
      import DefaultSeparators._
      val map = data.parseMap

      SourceRow(
        dt = map.get("dt"),
        uid = map.get("uid").map(_.toInt),
        uid_type = map.get("uid_type"),
        pFeature1 = map.get("pFeature1").map(_.toBoolean),
        pFeature2 = map.get("pFeature2"),
        pFeature3 = map.get("pFeature3").map(_.toDouble.toInt),
        pFeature4 = map.get("pFeature4").map(_.toFloat),
        pFeature5 = map.get("pFeature5").map(_.toDouble),
        mFeature = map.get("mFeature").map(_.parseMap(Separators(";", Some(Separators("="))))).map(m => m.mapValues(_.toDouble.toInt)),
        aFeature = map.get("aFeature").map(arrayWithNulls(_, ";")),
        id = map.get("id").map(_.toInt)
      )
    }

    def arrayWithNulls(lst: String, sep: String): Array[Option[Int]] = {
      import com.mrg.dm.toolbox.StringToolbox._
      import DefaultSeparators._

      lst.splitTrimNoFilter(sep)
        .map(itm => Try(itm.toDouble.toInt).toOption)
        .toArray
    }

  }
}

object Implicits {
  val UID_COL_NAME = "uid"
  val DT_COL_NAME = "dt"
  val UID_TYPE_COL_NAME = "uid_type"
  def keyColumns = Seq(UID_COL_NAME, DT_COL_NAME, UID_TYPE_COL_NAME)
}
