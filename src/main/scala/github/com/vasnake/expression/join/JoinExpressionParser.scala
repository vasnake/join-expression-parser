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

  sealed abstract class Expression
  case class Node(name: String) extends Expression
  case class Tree(left: Expression, right: Expression, joinOp: String) extends Expression

  def apply(rule: String): Expression = {
    ???
  }

}
