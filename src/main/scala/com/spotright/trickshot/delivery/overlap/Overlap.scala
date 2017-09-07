package com.spotright.trickshot.delivery.overlap

import scala.io.Source

import com.spotright.common.lib.PairingHeap

/**
  * com.spotright.overlap.Overlap file ...
  *
  * Generates an overlap report of the provided files -- pairwise overlap.
  *
  * Idea is to keep a priority queue values (by file).  We pull off all matching values from the top of
  * the queue and update the match-counts of those found.  (Pathologic no-match case means update our
  * own count to track file size.)
  *
  * Currently assumes input files are a sorted column of values which the program will uniq.
  */
object Overlap {

  val usage = "overlap file file ..."

  def main(av: Array[String]): Unit = {

    require(av.length > 0, usage)

    val files = av.zipWithIndex.map {
      case (fn, idx) =>
        val iter = Source.fromFile(fn).getLines().map {
          _.trim.toLowerCase
        }.filterNot {
          _.isEmpty
        }
        val fcc = FileCC("-", None, new java.io.File(fn).getName, idx, UniqIterator(iter.buffered), Array.fill(av.length)(0L))

        fcc.next()
    }

    val ph =
      PairingHeap.empty()(FileCC.HeadOnlyOrdering) ++
        files.filterNot {
          _.isDone
        }

    while (ph.nonEmpty) {
      val top = ph.dequeue()

      if (ph.isEmpty) {
        // No other files left.  Just increment our own index by our length.
        var count = 1 // head
        if (top.mnext.isDefined) count += 1
        count += top.lines.length
        top.counts(top.idx) += count
      }
      else if (ph.inspect().head != top.head) {
        // No match.  Update top's own count
        top.counts(top.idx) += 1

        if (top.hasNext) {
          val fcc = top.next()
          files(top.idx) = fcc
          ph += fcc
        }
      }
      else {
        // Grab all the matching files.
        val xsbldr = List.newBuilder[FileCC]
        xsbldr += top

        while (ph.nonEmpty && ph.inspect().head == top.head) {
          xsbldr += ph.dequeue()
        }

        val xs = xsbldr.result()
        val idxs = xs.map {
          _.idx
        }

        // Update counts.
        for {
          i <- idxs
          j <- idxs
        } {
          files(i).counts(j) += 1
        }

        // Grab next lines.
        xs.filter {
          _.hasNext
        }.foreach {
          x =>
            val fcc = x.next()
            files(x.idx) = fcc
            ph += fcc
        }
      }
    }


    // Overlap Report (percent)
    //
    //    filename,line count,%-match,%-match,...
    //
    // where %-match (i,j) is the percent of file i matched in j.

    println("__Overlap Report (percent)__")
    println(s"name,size,${
      files.map {
        _.name
      }.mkString(",")
    }")
    files.foreach {
      f =>
        val flen = f.counts(f.idx)
        // Divide every value by the file's own count
        print(s"${f.name},")
        print(s"$flen,")
        println(
          f.counts.map { n => (n.toDouble / flen * 100).formatted("%.2f") }.mkString(",")
        )
    }

    println()

    // Overlap Report (count)
    //
    //    filename,count,count,...
    //
    // where the (i,j)th count is the number of matches found between file i and file j.
    // For cell (i, i) it's the count of the number of lines in the file.

    println("__Overlap Report (count)__")
    println(s"name,${
      files.map {
        _.name
      }.mkString(",")
    }")
    files.foreach {
      f =>
        print(s"${f.name},")
        println(f.counts.mkString(","))
    }
  }
}
