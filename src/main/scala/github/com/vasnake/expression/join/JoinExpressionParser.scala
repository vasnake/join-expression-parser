/*
 * Copyright (c) 2020-2021, vasnake@gmail.com
 *
 * Licensed under GNU General Public License v3.0 (see LICENSE)
 *
 */

package github.com.vasnake.expression.join

object JoinExpressionParser {

  sealed abstract class Expression

  def apply(rule: String): Expression = {
    ???
  }

}
