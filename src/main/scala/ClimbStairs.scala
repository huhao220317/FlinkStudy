// 爬楼梯
// 每次可以爬1个台阶或者2个台阶
// 问 n 个台阶有几种爬的方法
object ClimbStairs {
  def main(args: Array[String]): Unit = {
    println(fibWithTailRec(100)) // 1298777728820984005
                                 // 1298777728820984005
    println(prod(5))
    println(prodWithTailRec(5))
  }

  // f(100) = f(99) + f(98) = f(98) + 2 * f(97) + f(96)
  // = f(97) + 3 * f(96) + 3 * f(95) + f(94)
  def fib(n: Int): Long = {
    if (n == 1 || n == 2) n
    else fib(n - 1) + fib(n - 2)
  }

  // 尾递归
  // loop(2, 0, 1)
  // loop(1, 1, 1)
  def fibWithTailRec(n: Int): Long = {
    @annotation.tailrec
    def loop(n: Int, prev: Long, cur: Long): Long =
      if (n == 1) cur
      else loop(n - 1, cur, prev + cur)
    loop(n, 1, 1)
  }

  def prod(n: Int): Long = {
    if (n == 1) 1
    else n * prod(n - 1)
  }

  def prodWithTailRec(n: Int): Long = {
    @annotation.tailrec
    def loop(n: Int, acc: Long): Long = {
      if (n == 1) acc
      else loop(n - 1, n * acc)
    }
    loop(n, 1)
  }
}