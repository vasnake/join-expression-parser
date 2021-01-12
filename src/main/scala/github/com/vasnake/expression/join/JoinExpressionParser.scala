/*
 * Copyright (c) 2020-2021, vasnake@gmail.com
 *
 * Licensed under GNU General Public License v3.0 (see LICENSE)
 *
 */

package github.com.vasnake.expression.join

sealed trait JoinExpression[T] {
  def eval[B](op: (B, B, String) => B)(implicit conv: String => T, ev: T => B): B
}

case class SingleItemJoin[T](item: T) extends JoinExpression[T] {
  override def eval[B](op: (B, B, String) => B)(implicit conv: String => T, ev: T => B): B =
    ev(item)
}

case class TreeJoin[T](tree: JoinExpressionParser.Tree) extends JoinExpression[T] {
  override def eval[B](op: (B, B, String) => B)(implicit conv: String => T, ev: T => B): B = {
    import JoinExpressionParser.{Tree, Node}

    tree match {
      case Tree(left: Node, right: Node, jo) => op(conv(left.name), conv(right.name), jo)
      case Tree(left: Node, right: Tree, jo) => op(conv(left.name), TreeJoin[T](right).eval(op)(conv, ev), jo)
      case Tree(left: Tree, right: Node, jo) => op(TreeJoin[T](left).eval(op)(conv, ev), conv(right.name), jo)
      case Tree(left: Tree, right: Tree, jo) => op(TreeJoin[T](left).eval(op)(conv, ev), TreeJoin[T](right).eval(op)(conv, ev), jo)
    }
  }
}

object JoinExpressionParser {
  import scala.util.parsing.combinator.RegexParsers

  sealed abstract class Expression
  case class Node(name: String) extends Expression
  case class Tree(left: Expression, right: Expression, joinOp: String) extends Expression

  def apply(rule: String): Expression = {
    val p = new Parser
    p.parseAll(p.expr, rule) match {
      case p.Success(result: Expression, _) => result
      case e @ p.NoSuccess(_, _) => throw new IllegalArgumentException(s"JoinRuleParser has failed: `${e.toString}`")
    }
  }

  class Parser extends RegexParsers {
    // expr = operand (operator operand)+
    // operand = name | (expr)

    def expr: Parser[Expression] = operand ~ rep(operator ~ operand) ^^ {
      case left ~ lst => makeTree(left, lst)
    }

    def operand: Parser[Expression] = name ^^ { n => Node(n) } | "(" ~> expr <~ ")"
    def operator: Parser[String] = """\w+""".r
    def name: Parser[String] = """\w+""".r

    @scala.annotation.tailrec
    private def makeTree
    (
      left: Expression,
      rest: List[String ~ Expression]
    ): Expression =
      rest match {
        case Nil => left
        case h :: t => makeTree(Tree(left, h._2, h._1), t)
      }

  }

}
