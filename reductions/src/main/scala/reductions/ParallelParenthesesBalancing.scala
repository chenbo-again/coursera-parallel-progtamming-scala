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
  def balance(chars: Array[Char]): Boolean = {
    var t = 0
    for (c <- chars) {
      if(c == '(') {
        t += 1
      }
      else if(c == ')') {
        t -= 1
      }
      if(t < 0) {
        return false
      }
    }
    t == 0
  }
  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  // 最小值和最终结果是最重要的
  def parBalance(chars: Array[Char], threshold: Int): Boolean =

    def traverse(idx: Int, until: Int): (Int, Int) = {
      var (begin, end) = (0, 0)
      
      var state = 0
      var i = idx
      while(i < until) {
        if(chars(i) == '(') {
          state += 1
        } else if(chars(i) == ')') {
          if(state > 0) {
            state -= 1
          } else {
            begin += 1
          }
        }
        
        i += 1
      }
      
      state = 0
      i = until - 1
      while(i >= idx) {
        if(chars(i) == '(') {
          if(state > 0) {
            state -= 1
          } else {
            end += 1
          }
        } else if(chars(i) == ')'){
          state += 1
        }
        
        i -= 1
      }
  
      (begin, end)
    }
    
    def reduce(from: Int, until: Int): (Int, Int) = {
      if(until - from <= threshold) traverse(from, until)
      else {
        val mid = (from + until)/2
        val ((lb, le), (rb, re)) = parallel(traverse(from, mid), traverse(mid, until))
        
        if(le < rb) {
          (lb + rb - le, re)
        } else {
          (lb, re + le - rb)
        }
      }
      
    }

    reduce(0, chars.length) == (0, 0)
  // For those who want more:
  // Prove that your reduction operator is associative!

