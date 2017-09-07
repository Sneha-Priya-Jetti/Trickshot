package com.spotright.trickshot.delivery.overlap

case class FileCC(
                   head: String,
                   mnext: Option[String], // used to remove duplicates
                   name: String,
                   idx: Int,
                   lines: UniqIterator[String], // access only via non-mutable FileCC.next
                   counts: Array[Long] // mutable - number of matches with file at index
                 ) {

  def isDone: Boolean = head.isEmpty

  def hasNext: Boolean = mnext.isDefined || lines.hasNext

  def next(): FileCC = {
    if (isDone) this
    else if (mnext.isDefined) this.copy(head = mnext.get, mnext = None)
    else {
      var newhead = ""

      while (newhead.isEmpty && lines.hasNext) {
        newhead = lines.next()
      }

      var newnext = newhead

      while ((newnext == newhead || newnext.isEmpty) && lines.hasNext) {
        newnext = lines.next()
      }

      val newmnext = if (newnext == newhead) None else Some(newnext)

      if (newhead != "")
        this.copy(head = newhead, mnext = newmnext)
      else
        throw new NoSuchElementException("next on empty iterator")
    }
  }
}

object FileCC {

  /** Compare by current value.  If equal order by index. */
  object HeadOnlyOrdering extends Ordering[FileCC] {
    def compare(a: FileCC, b: FileCC): Int = {
      val v = a.head compare b.head
      if (v != 0) v else a.idx compare b.idx
    }
  }

}
