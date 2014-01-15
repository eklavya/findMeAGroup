import com.typesafe.config.ConfigFactory
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

  def calcBetweenness(nodes: Seq[Int]): Array[List[Neighbour]]

  private[this] var foundId = -2

  private[this] var comMap = Map.empty[Int, (MaxBetweenness, Double)]
  private[this] var finalMap = Map.empty[Int, (MaxBetweenness, Double)]

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
    (0 until numVertices).par.foreach { i => if (communities(i) > -1) communities(i) = -1 }

    var count = 0

    def dfsTraverse(v: Int) {
      communities(v) = count
      graph(v).foreach { n =>
        if (communities(n.node) == -1) {
          dfsTraverse(n.node)
        }
      }
    }

    (0 until numVertices).foreach { v =>
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

    comMap = Map.empty[Int, (MaxBetweenness, Double)]

    val count = markCommunities

    val parFac = ConfigFactory.load.getInt("parallel-factor")

    println(s"found ${finalMap.size} communities")

    if (count > 0) {

      (0 until count).par.foreach { i =>
        val nodes = communities.indices.filter(communities(_) == i)
        val accs = nodes.groupBy(_ % parFac).values.par.map(calcBetweenness(_))

        var maxBetweenness = MaxBetweenness(-1, Neighbour(0, 0.0))

        val asd = Array.fill[List[Neighbour]](numVertices)(List[Neighbour]())

        graph.zipWithIndex.par.foreach { case (el, i) =>
          el foreach { n =>
            asd(i) = Neighbour(n.node, 0.0) :: asd(i)
          }
        }

        val res = accs.fold(asd) { case(g, a) => asd.indices.foreach { n =>
          (a(n) zip g(n)) foreach { case(e1, e2) =>
            e1.betweenness += e2.betweenness
            maxBetweenness = if (maxBetweenness.nbr.betweenness > e1.betweenness) maxBetweenness else MaxBetweenness(n, e1)
          }
        }
          a
        }

        val v = nodes.flatMap(res(_))
        val mean = nodes.flatMap(res(_)).map(_.betweenness).foldRight(0.0)(_ + _) / v.length
        comMap = comMap.updated(i, (MaxBetweenness(maxBetweenness.node, Neighbour(maxBetweenness.nbr.node, maxBetweenness.nbr.betweenness)), mean))
      }

      comMap.foreach { case(i, (max, mean)) =>
        println(s"for community $i max ${max.nbr.betweenness}, mean $mean coeff ${max.nbr.betweenness / mean}")
      }

      val coeff = ConfigFactory.load.getDouble("coeff")

      comMap.foreach { case(i, (max, mean)) =>
          if ((max.nbr.betweenness / mean) < coeff) {
            markFound(i)
          } else if (max.node > -1) {
            val j = max.node
            val k = max.nbr.node
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