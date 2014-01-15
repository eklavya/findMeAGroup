import akka.actor.{Props, ActorSystem, Actor}
import com.typesafe.config.ConfigFactory

/**
 * Created by eklavya on 8/1/14.
 */

//get graph
//update graph
//calcBetweenness
//send result

case class Break(v1: Int, v2: Int)

class Processor extends Actor {

  val parFac = ConfigFactory.load.getInt("processor.parFac")

  val inputEdges = io.Source.fromFile(ConfigFactory.load.getString("graphInput")).getLines.toList

  val numVertices = inputEdges.flatMap(_.split(",")).distinct.size

  val graph = {
    val graph = new Graph(numVertices)
    inputEdges.map {
      el =>
        val e = el.split(",")
        (e(0).toInt, e(1).toInt)
    }.foreach { edge =>
      graph.addEdge(edge._1, edge._2)
    }
    graph
  }

  override def preStart {
//    println(s"number of nodes in this graph - ${graph.graph.size}")
    markCommunities
  }

  def markCommunities {
    (0 until numVertices).par.foreach { i => if (graph.communities(i) > -1) graph.communities(i) = -1 }

    var count = 0

    def dfsTraverse(v: Int) {
      graph.communities(v) = count
      graph.graph(v).foreach { n =>
        if (graph.communities(n.node) == -1) {
          dfsTraverse(n.node)
        }
      }
    }

    (0 until numVertices).foreach { v =>
      if (graph.communities(v) == -1) {
        dfsTraverse(v)
        count += 1
      }
    }
  }


  def receive = {

    case Break(v1, v2) =>
      println(s"Break edge between $v1 and $v2")
      graph.graph(v1) = graph.graph(v1).filter(_.node != v2)
      graph.graph(v2) = graph.graph(v2).filter(_.node != v1)
      graph.acc(v1) = graph.acc(v1).filter(_.node == v2)
      graph.acc(v2) = graph.acc(v2).filter(_.node == v1)
      markCommunities

    case Calc(nodes: Seq[Int]) =>
      val s = sender
      println("Received work, working...")
      val accs = nodes.groupBy(_ % parFac).values.par.map(graph.calcBetweenness(_))
      val asd = Array.fill[List[Neighbour]](numVertices)(List[Neighbour]())

      graph.graph.zipWithIndex.par.foreach { case (el, i) =>
        el foreach { n =>
          asd(i) = Neighbour(n.node, 0.0) :: asd(i)
        }
      }

      accs.foldLeft(asd) { case(a, g) => asd.indices.foreach { n =>
        (a(n) zip g(n)) foreach { case(e1, e2) =>
          e1.betweenness += e2.betweenness
        }
      }
        a
      }
      println("sending back result")
      s !	ResultArray(asd)
  }
}

object Processor extends App {
  val system = ActorSystem("ClusterSystem")
  val proc = system.actorOf(Props[Processor], "processor")
}