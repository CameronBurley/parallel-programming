package reductions

import scala.annotation.*
import org.scalameter.*

object ParallelParenthesesBalancingRunner:

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns := 40,
    Key.exec.maxWarmupRuns := 80,
    Key.exec.benchRuns := 120,
    Key.verbose := false
  ) withWarmer(Warmer.Default())

  def main(args: Array[String]): Unit =
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime")
    println(s"speedup: ${seqtime.value / fjtime.value}")

object ParallelParenthesesBalancing extends ParallelParenthesesBalancingInterface:

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean =
    @tailrec
    def balanceRec(accum: Int = 0, chars: Array[Char] = chars): Boolean =
      accum match
        case accum if accum <= -1 => false
        case _ => chars match
          case Array() => accum == 0
          case _ =>
            chars.head match
              case '(' =>
                balanceRec(accum + 1, chars.tail)
              case ')' =>
                balanceRec(accum - 1, chars.tail)
              case _ =>
                balanceRec(accum, chars.tail)
    balanceRec()


  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean =


    @tailrec
    def traverse(idx: Int, until: Int, open: Int, closed: Int): (Int, Int) =
      if (idx >= until)
        (open, closed)
      else
        chars(idx) match
          case '(' => traverse(idx + 1, until, open + 1, closed)
          case ')' => if (open >= 1) traverse(idx + 1, until, open - 1, closed)
            else traverse(idx + 1, until, open, closed + 1)
          case _ => traverse(idx + 1, until, open, closed)

    def reduce(from: Int, until: Int): (Int, Int) =
      if (until - from <= threshold || until - from <= 1)
        traverse(from, until, 0, 0)
      else
        val mid = from + ((until - from) / 2)
        val ((leftOpen, leftClosed), (rightOpen, rightClosed)) = parallel(reduce(from, mid), reduce(mid, until))
        val min = Math.min(leftOpen, rightClosed)
        ((leftOpen - min) + rightOpen, (rightClosed - min) + leftClosed)

    def reduce2(from: Int, until: Int): (Int, Int) =
      if (until - from <= threshold || until - from <= 1)
        traverse(from, until, 0, 0)
      else
        val mid = from + ((until - from) / 2)
        val ((leftOpen, leftClosed), (rightOpen, rightClosed)) = parallel(reduce(from, mid), reduce(mid, until))
        if (leftOpen <= rightClosed)
          (rightOpen, (rightClosed - leftOpen) + leftClosed)
        else
          ((leftOpen - rightClosed) + rightOpen, leftClosed)

    reduce(0, chars.length) == (0, 0)

  // For those who want more:
  // Prove that your reduction operator is associative!

