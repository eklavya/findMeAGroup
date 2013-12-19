import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by eklavya on 13/12/13.
 */

trait Communities {

  val communities: Array[Int]
  val numVertices: Int
  val graph: Array[List[Neighbour]]
  val acc: Array[List[Neighbour]]

  def calcBetweenness(nodes: Seq[Int]): ((Int, Neighbour), Double)

  private[this] var foundId = -2

  private[this] var comMap = Map.empty[Int, ((Int, Neighbour), Double)]
  private[this] var finalMap = Map.empty[Int, ((Int, Neighbour), Double)]

  def getFoundId = {
    foundId -= 1
    foundId + 1
  }

  def printCommunities {
    communities.indices groupBy {
      i => communities(i)
    } foreach println
  }

  def markFound(i: Int) {
    val id = getFoundId

    //Mark all the vertices in this connected component as a found community
    (0 until numVertices) foreach { x => if (communities(x) == i) communities(x) = id }

    //update community map with the new found community
    finalMap.updated(id, comMap(i))
  }

  def markCommunities = {
    /*
    only mark if they are not a found community, this helps by removing the need to recalculate
    max and min betweenness lists for already found communities
     */
    (0 until numVertices) foreach { i => if (communities(i) > -1) communities(i) = -1 }

    var count = 0

    def dfsTraverse(v: Int) {
      communities(v) = count
      graph(v) foreach { n =>
        if (communities(n.node) == -1) {
          dfsTraverse(n.node)
        }
      }
    }

    (0 until numVertices) foreach { v =>
      if (communities(v) == -1) {
        dfsTraverse(v)
        count += 1
      }
    }

    println(s"Communities left to analyze now : $count")
    count
  }

  @tailrec
  final def findCommunities {

    comMap = Map.empty[Int, ((Int, Neighbour), Double)]

    val count = markCommunities

    println(s"found ${finalMap.size} communities")

    if (count > 0) {

      (0 until count) foreach { i =>
        val nodes = communities.indices.filter(communities(_) == i)
        val accs = nodes.groupBy(_ % 4).values.par.map(calcBetweenness(_))
        val max = accs.foldRight(accs.head)((a, ac) => if (a._1._2.betweenness > ac._1._2.betweenness) a else ac)
        val mean = accs.foldRight(0.0)((m, mc) => m._2 + mc) / accs.size
        comMap = comMap.updated(i, (max._1, mean))
      }

      comMap.foreach { case(i, (max, median)) =>
        println(s"for community $i avg(max) ${max._2.betweenness}, median $median coeff ${max._2.betweenness / median}")
      }

      comMap.foreach { case(i, (max, median)) =>
          if ((max._2.betweenness/median) < 2.0) {
            markFound(i)
          } else if (max._1 > -1) {
            val j = max._1
            val k = max._2.node
            graph(j) = graph(j).filter(_.node != k)
            graph(k) = graph(k).filter(_.node != j)
            acc(j) = acc(j).filter(_.node != k)
            acc(k) = acc(k).filter(_.node != j)
          }
      }
      findCommunities
    }
  }
}