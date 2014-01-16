/**
 * Created by eklavya on 6/12/13.
 */

import org.scalatest.{ShouldMatchers, FlatSpec}
import scala.collection.mutable.ArrayBuffer

class GraphSpec extends FlatSpec with ShouldMatchers {

  val inputEdges = io.Source.fromURL(getClass.getResource("/graph")).getLines.toList

  val numVertices = inputEdges.flatMap(_.split(",")).distinct.size

  val graph = {
    val graph = new Graph(numVertices)
    inputEdges.map { el =>
      val e = el.split(",")
      (e(0).toInt, e(1).toInt)
    } foreach(edge => graph.addEdge(edge._1, edge._2))
    graph
  }

  "Graph" should "be properly built" in {
    graph.getEdges(0)  should be(List(Neighbour(6,  0.0), Neighbour(5,  0.0), Neighbour(2, 0.0), Neighbour(1, 0.0)))
    graph.getEdges(1)  should be(List(Neighbour(0,  0.0)))
    graph.getEdges(2)  should be(List(Neighbour(0,  0.0)))
    graph.getEdges(3)  should be(List(Neighbour(5,  0.0), Neighbour(4, 0.0)))
    graph.getEdges(4)  should be(List(Neighbour(6,  0.0), Neighbour(5,  0.0), Neighbour(3, 0.0)))
    graph.getEdges(5)  should be(List(Neighbour(4,  0.0), Neighbour(3,  0.0), Neighbour(0, 0.0)))
    graph.getEdges(6)  should be(List(Neighbour(4,  0.0), Neighbour(0,  0.0)))
    graph.getEdges(7)  should be(List(Neighbour(8, 0.0)))
    graph.getEdges(8)  should be(List(Neighbour(7,  0.0)))
    graph.getEdges(9)  should be(List(Neighbour(12, 0.0), Neighbour(11, 0.0), Neighbour(10, 0.0)))
    graph.getEdges(10) should be(List(Neighbour(9, 0.0)))
    graph.getEdges(11) should be(List(Neighbour(12, 0.0), Neighbour(9,  0.0)))
    graph.getEdges(12) should be(List(Neighbour(11, 0.0), Neighbour(9,  0.0)))
  }

  "Graph" should "have valid shortest path values" in {
    val distance    = Array.fill[Int](numVertices)(-1)
    val weights     = new Array[Int](numVertices)
    val arrivedFrom = new Array[Int](numVertices)

    distance(0)     = 0
    weights(0)      = 1

    graph.calcShortestPathTree(graph.graph, 0, distance, weights, arrivedFrom)

    distance(1)  should be(1)
    distance(2)  should be(1)
    distance(3)  should be(2)
    distance(4)  should be(2)
    distance(5)  should be(1)
    distance(6)  should be(1)
    distance(7)  should be(-1)
    distance(8)  should be(-1)
    distance(9)  should be(-1)
    distance(10) should be(-1)
    distance(11) should be(-1)
    distance(12) should be(-1)
  }

  "Graph" should "report valid leaves" in {
    val distance    = Array.fill[Int](numVertices)(-1)
    val weights     = new Array[Int](numVertices)
    val arrivedFrom = new Array[Int](numVertices)

    distance(0)     = 0
    weights(0)      = 1

    graph.markCommunities

    val shortestPathNodeList = graph.calcShortestPathTree(graph.graph, 0, distance, weights, arrivedFrom)

    graph.calcLeaves(shortestPathNodeList, 0) should be(ArrayBuffer(1, 2, 3, 4))
  }

  "Graph" should "calculate correct betweenness for nodes" in {
    var comMap = Map.empty[Int, (MaxBetweenness, Double)]

    (0 until 3).par.foreach { i =>
      val nodes = graph.communities.indices.filter(graph.communities(_) == i)
      val accs = nodes.groupBy(_ % 4).values.par.map(graph.calcBetweenness(_))

      var maxBetweenness = MaxBetweenness(-1, Neighbour(0, 0.0))

      val asd = Array.fill[List[Neighbour]](numVertices)(List[Neighbour]())

      graph.graph.zipWithIndex.par.foreach { case (el, i) =>
        el foreach { n =>
          asd(i) = Neighbour(n.node, 0.0) :: asd(i)
        }
      }

      accs foreach { a =>
        a.foreach { x => print(s"$x, ")
          println
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

    }
  }
}


