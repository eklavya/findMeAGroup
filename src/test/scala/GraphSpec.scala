/**
 * Created by eklavya on 6/12/13.
 */

import org.scalatest.{ShouldMatchers, FlatSpec}

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
    graph.getEdges(0) should be(List(Edge(3, 0.0), Edge(2, 0.0), Edge(1, 0.0)))
    graph.getEdges(1) should be(List(Edge(4, 0.0), Edge(3, 0.0), Edge(0, 0.0)))
    graph.getEdges(2) should be(List(Edge(6, 0.0), Edge(3, 0.0), Edge(0, 0.0)))
    graph.getEdges(3) should be(List(Edge(6, 0.0), Edge(5, 0.0), Edge(4, 0.0), Edge(2, 0.0), Edge(1, 0.0), Edge(0, 0.0)))
    graph.getEdges(4) should be(List(Edge(3, 0.0), Edge(1, 0.0)))
    graph.getEdges(5) should be(List(Edge(6, 0.0), Edge(3, 0.0)))
    graph.getEdges(6) should be(List(Edge(7, 0.0), Edge(5, 0.0), Edge(3, 0.0), Edge(2, 0.0)))
    graph.getEdges(7) should be(List(Edge(10, 0.0), Edge(9, 0.0), Edge(8, 0.0), Edge(6, 0.0)))
    graph.getEdges(8) should be(List(Edge(9, 0.0), Edge(7, 0.0)))
    graph.getEdges(9) should be(List(Edge(12, 0.0), Edge(11, 0.0), Edge(8, 0.0), Edge(7, 0.0)))
    graph.getEdges(10) should be(List(Edge(12, 0.0), Edge(11, 0.0), Edge(7, 0.0)))
    graph.getEdges(11) should be(List(Edge(10, 0.0), Edge(9, 0.0)))
    graph.getEdges(12) should be(List(Edge(10, 0.0), Edge(9, 0.0)))
  }

  "Graph" should "have valid shortest path values" in {
    val distance    = new Array[Int](numVertices)
    val weights     = new Array[Int](numVertices)
    val arrivedFrom = new Array[Int](numVertices)
    distance(0) = 0
    weights(0)  = 1

    graph.calcShortestPathTree(0, distance, weights, arrivedFrom)

    distance(1) should be(1)
    distance(2) should be(1)
    distance(3) should be(1)
    distance(4) should be(2)
    distance(5) should be(2)
    distance(6) should be(2)
    distance(7) should be(3)
    distance(8) should be(4)
    distance(9) should be(4)
    distance(10) should be(4)
    distance(11) should be(5)
    distance(12) should be(5)
  }
}
