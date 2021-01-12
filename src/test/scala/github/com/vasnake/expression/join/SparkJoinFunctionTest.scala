/*
 * Copyright (c) 2020-2021, vasnake@gmail.com
 *
 * Licensed under GNU General Public License v3.0 (see LICENSE)
 *
 */

package github.com.vasnake.expression.join

import scala.util.Try

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

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

    val rule = parseJoinRule("foo", "bar")

    val expected = Seq(
      "[2020-05-05,42,OKID,true,foo,null,null,null,null,null,null]"
    )

    val actual = joinWithAliases(tables, rule)
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

    val rule = parseJoinRule("a outer (b left_outer c) inner d", "")

    val expected: Seq[String] = Seq(
      "[42,OKID,true,2,3,4.0]"
    )

    val actual = joinWithAliases(tables, rule).collect.map(_.toString)

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

    val rule = parseJoinRule("a inner b inner c inner d", "")

    val df = joinWithAliases(tables, rule)
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

    def join(rule: String): Seq[String] = joinWithAliases(tables, parseJoinRule(rule, "")).collect.map(_.toString).toSeq

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

    val rule = parseJoinRule("(a left_outer b) inner (c right_outer d)", "")

    val expected: Seq[String] = Seq(
      "[41,OKID,true,null,null,4.0]",
      "[42,OKID,true,2,3,4.0]"
    )

    val actual = joinWithAliases(tables, rule).collect.map(_.toString)

    actual should contain theSameElementsAs expected
  }
}

object SparkJoinFunctionTest {

  import sql.DataFrame
  import Implicits._

  def joinWithAliases(tables: Map[String, DataFrame], joinTree: JoinExpression[String]): DataFrame = {
    // join-result = (join-tree, df-catalog) => df
    JoinRule.join(
      tree = joinTree,
      catalog = name => tables(name).as(name),
      keys = uidKeyPair
    )
  }

  def parseJoinRule(rule: String, defaultItem: String): JoinExpression[String] = {
    // join rule: "topics full_outer (profiles left_outer groups)"
    // or "" or "topics_composed"
    import github.com.vasnake.toolbox.StringToolbox._

    rule.splitTrim(Separators(" ")).toSeq match {
      case Seq() => SingleItemJoin(defaultItem.trim)
      case Seq(item) => SingleItemJoin(item)
      case Seq(_, _) => throw new IllegalArgumentException(s"Invalid config: malformed join rule `${rule}`")
      case _  => JoinRule.parse(rule)
    }
  }

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
    import github.com.vasnake.toolbox.StringToolbox._
    import DefaultSeparators._

    def apply(data: String): SourceRow = {
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
      lst.splitTrimNoFilter(sep)
        .map(itm => Try(itm.toDouble.toInt).toOption)
        .toArray
    }

  }
}

object Implicits {
  import sql.DataFrame

  val UID_COL_NAME = "uid"
  val DT_COL_NAME = "dt"
  val UID_TYPE_COL_NAME = "uid_type"
  def keyColumns = Seq(UID_COL_NAME, DT_COL_NAME, UID_TYPE_COL_NAME)
  def uidKeyPair = Seq(UID_COL_NAME, UID_TYPE_COL_NAME)

  implicit class RichDataset(val ds: DataFrame) extends AnyVal {

    import sql.functions.{col, lit, expr}
    import sql.types.{LongType, DataType}

    def setColumnsInOrder(withDT: Boolean, withUT: Boolean): DataFrame = {
      val dataCols = ds.columns.filter(n => !keyColumns.contains(n)).toSeq

      ds.select(
        UID_COL_NAME,
        dataCols ++
          (if (withDT) Seq(DT_COL_NAME) else Seq.empty) ++
          (if (withUT) Seq(UID_TYPE_COL_NAME) else Seq.empty)
          : _*)
    }

    def filterDTPartition(dt: String): DataFrame = ds.where(col(DT_COL_NAME) === lit(dt))

    def filterUidTypePartitions(utypes: Option[List[String]]): DataFrame = {
      utypes.map(
        lst => ds.where(col(UID_TYPE_COL_NAME).isin(lst: _*))
      ).getOrElse(ds)
    }

    def filterPartitions(partitions: List[Map[String, String]]): DataFrame = {
      import sql.Column

      // and
      def onePartitionFilter(row: Map[String, String]): Column = row.foldLeft(col(DT_COL_NAME).isNotNull){
        case (acc, (k, v)) => acc and (col(k) === lit(v))
      }

      val filters: List[Column] = partitions map onePartitionFilter

      // or
      val combinedFilter: Column = filters.tail.foldLeft(filters.head) {
        case (acc, filter) => acc or filter
      }

      ds.where(combinedFilter)
    }

    def optionalWhere(filterExpr: Option[String]): DataFrame = filterExpr.map(f => ds.where(f)).getOrElse(ds)

    def dropInvalidUID: DataFrame = {
      // drop record where: uid_type in (OKID, VKID) and (uid is null or uid <= 0)
      val condition = col(UID_COL_NAME).isNull or {
        col(UID_TYPE_COL_NAME).isin("OKID", "VKID") and
          col(UID_COL_NAME).cast(LongType) <= 0L
      }

      ds.where(!condition)
    }

    def selectFeatures(features: Option[List[String]]): DataFrame = {
      if (features.getOrElse(List.empty[String]).nonEmpty) features else None
    }.map(fs => ds.select(
      col(UID_COL_NAME) +:
        col(UID_TYPE_COL_NAME) +:
        fs.map(f => expr(f))
        : _*).dropRepeatedCols
    ).getOrElse(ds)

    def dropRepeatedCols: DataFrame = {
      import scala.collection.mutable
      val names = mutable.LinkedHashSet.empty[String]
      for (f <- ds.schema) names += f.name

      ds.select(names.toList.map(col): _*)
    }

    def dropPartitioningCols(partitions: List[Map[String, String]], except: Set[String]): DataFrame = {
      val colnames = partitions.flatMap(p => p.keys)
        .toSet.toSeq.filter(cn => !except.contains(cn))

      ds.drop(colnames: _*)
    }

    def castColumnTo(colname: String, coltype: DataType): DataFrame = {
      val columns = ds.schema.map(f =>
        if(f.name.toLowerCase == colname.toLowerCase) col(f.name).cast(coltype) else col(f.name)
      )

      ds.select(columns: _*)
    }

    def selectCSVcols(csv: String, sep: String = ","): DataFrame = {
      import github.com.vasnake.toolbox.StringToolbox._
      import DefaultSeparators._

      ds.selectExpr(csv.splitTrim(sep): _*)
    }
  }
}

object JoinRule {

  type DF = sql.DataFrame
  type JE = JoinExpression[String]

  def parse(rule: String): JE = _parse[String](rule)(identity)

  private def enumerateItems(tree: JE): Seq[String] = _enumerateItems[String](tree)(identity)

  def join(tree: JE, catalog: String => DF, keys: Seq[String] = Seq("uid", "uid_type")): DF = {
    tree.eval[DF] { case (left, right, join) =>
      left.join(right, keys, join)
    }(identity, catalog)
  }

  private def _parse[T](rule: String)(implicit conv: String => T): JoinExpression[T] = JoinExpressionParser(rule) match {
    case JoinExpressionParser.Node(name) => SingleItemJoin[T](conv(name))
    case tree: JoinExpressionParser.Tree => TreeJoin[T](tree)
  }

  private def _enumerateItems[T](tree: JoinExpression[T])(implicit conv: String => T): Seq[T] = {
    implicit val ev: T => Seq[T] = t => Seq(t)

    tree.eval[Seq[T]] {
      case (left, right, _) => left ++ right
    }
  }

}
