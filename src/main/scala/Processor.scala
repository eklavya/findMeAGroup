import akka.actor.{Props, ActorSystem, Actor}
import com.typesafe.config.ConfigFactory
import java.util.concurrent.Executors
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/**
 * Created by eklavya on 8/1/14.
 */

//get graph
//update graph
//calcBetweenness
//send result

case class Break(v1: Int, v2: Int)

class Processor(val graph: Graph) extends Actor {

  val parFac = ConfigFactory.load.getInt("processor.parFac")
  val executorService = Executors.newFixedThreadPool(4)
  val executionContext = ExecutionContext.fromExecutorService(executorService)

  def receive = {

    case Break(v1, v2) =>
      println(s"Break edge between $v1 and $v2")
      graph.graph(v1) = graph.graph(v1).filter(_.node != v2)
      graph.graph(v2) = graph.graph(v2).filter(_.node != v1)
      graph.acc(v1) = graph.acc(v1).filter(_.node == v2)
      graph.acc(v2) = graph.acc(v2).filter(_.node == v1)
      graph.markCommunities

    case Calc(nodes: Seq[Int]) =>
      val s = sender
      println("Received work, working...")

      val f = Future {
        val accs = nodes.groupBy(_ % parFac).values.par.map(graph.calcBetweenness(_)).seq

        accs.tail.foldLeft(accs.head) { case(a, g) => g.indices.foreach { n =>
          (a(n) zip g(n)) foreach { case(e1, e2) =>
            e1.betweenness += e2.betweenness
          }
        }
          a
        }
      }(executionContext)
      f.onComplete{ asd =>
        println("sending back result")
        s !	ResultArray(asd.get)
      }(executionContext)
  }
}

object Processor extends App {
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

  graph.markCommunities

  val conf = ConfigFactory.parseString("""akka.cluster.roles=["processor"]""").withFallback(ConfigFactory.load())
  val system = ActorSystem("ClusterSystem", conf)
  val proc = system.actorOf(Props(new Processor(graph)), "processor")
}