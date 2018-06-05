package ua.edu.ucu.cs.parallel

import org.scalameter
import org.scalameter.{Key, config}

import scala.io.Source

object ParallelWordsCount {

  trait Monoid[A] {
    def op(x: A, y: A): A
    def zero: A
  }

  def foldMapSegment[A, B](segment: IndexedSeq[A], from: Int, to: Int, m: Monoid[B])(transform: A => B): B = {
    var result = transform(segment(from))
    var index = from + 1
    while (index < to) {
      result = m.op(result, transform(segment(index)))
      index = index + 1
    }
    result
  }

  def foldMapPar[A, B](xs: IndexedSeq[A], from: Int, to: Int, m: Monoid[B])(f: A => B)
                      (implicit threshold: Int): B = {
    if (to - from < threshold)
      foldMapSegment(xs, from, to, m)(f)
    else {
      val middle = from + (to - from) / 2
      val (l, r) = parallel(
        foldMapPar(xs, from, middle, m)(f)(threshold),
        foldMapPar(xs, middle, to, m)(f)(threshold))
      m.op(l, r)
    }
  }

  sealed trait WordCount {
    def toInt: Int = this match {
      case Separator(s) => if (s) 1 else 0
      case WordCountTuple(ls, w, rs) => (if (ls) 1 else 0) + w + (if(rs) 1 else 0)
    }
  }
  case class Separator(isSeparator: Boolean) extends WordCount
  case class WordCountTuple(isLeftSeparator: Boolean, wordsCount: Int, isRightSeparator: Boolean) extends WordCount

  val wordCountMonoid = new Monoid[WordCount] {
    override def op(left: WordCount, right: WordCount): WordCount = (left, right) match {
      case (Separator(s1), Separator(s2)) => Separator(s1 || s2)
      case (Separator(s1), WordCountTuple(ls2, w2, rs2)) => WordCountTuple(s1 || ls2, w2, rs2)
      case (WordCountTuple(ls1, w1, rs1), Separator(s2)) => WordCountTuple(ls1, w1, rs1 || s2)
      case (WordCountTuple(ls1, w1, rs1), WordCountTuple(ls2, w2, rs2)) => {
        val isMidSeparator = rs1 || ls2
        WordCountTuple(ls1, w1 + w2 + (if (isMidSeparator) 1 else 0), rs2)
      }
    }
    override val zero = Separator(false)
  }

  def wordCountFunc(char: Char): WordCount =
    if (" ,.!?" contains char) Separator(true) else WordCountTuple(isLeftSeparator=false, 0, isRightSeparator=false)

  def wordCountSeq(string: String): Int =
    foldMapSegment(string, 0, string.length, wordCountMonoid)(wordCountFunc).toInt

  def wordCountPar(sentence: String): Int =
    foldMapPar(sentence, 0, sentence.length, wordCountMonoid)(wordCountFunc).toInt

  implicit val threshold: Int = 100

  def main(args: Array[String]) {

    val bufferedSource = Source.fromFile("src/main/data/text.txt")
    val text = bufferedSource.getLines.mkString(" ")
    bufferedSource.close

    println(s"Count sequential:   ${wordCountSeq(text)}")
    println(s"Count parallel:     ${wordCountPar(text)}")

    //    compare performance using Scala meter
    val standartConfig = config(
      Key.exec.minWarmupRuns -> 100,
      Key.exec.maxWarmupRuns -> 300,
      Key.exec.benchRuns -> 100,
      Key.verbose -> true
    ) withWarmer new scalameter.Warmer.Default


    val seqtime = standartConfig.measure {
      wordCountSeq(text)
    }

    val partime = standartConfig.measure {
      wordCountPar(text)
    }

    println(s"sequential time $seqtime ms")
    println(s"parallel time $partime ms")
    println(s"speedup: ${seqtime.value / partime.value}")
  }
}
