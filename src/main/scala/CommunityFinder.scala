import com.typesafe.config._
import akka.actor._
import akka.actor.RootActorPath
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.ClusterEvent.UnreachableMember
import akka.util.Timeout
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.collection.mutable.Set
import akka.pattern.ask

case object LiveNodes

object CommunityFinder {
  val inputEdges = io.Source.fromFile(ConfigFactory.load.getString("graphInput")).getLines.toList

  val numVertices = inputEdges.flatMap(_.split(",")).distinct.size

  val graph = {
    val graph = new Graph(numVertices)
    inputEdges.map { el =>
      val e = el.split(",")
      (e(0).toInt, e(1).toInt)
    } foreach(edge => graph.addEdge(edge._1, edge._2))
    graph
  }

  def main(args: Array[String]): Unit = {
    implicit val timeout = Timeout(10 seconds)

    // Override the configuration of the port
    // when specified as program argument
//    if (args.nonEmpty)
    val conf = ConfigFactory.parseString("akka.remote.netty.tcp.port=2551").withFallback(ConfigFactory.load())

    // Create an Akka system
    val system = ActorSystem("ClusterSystem", conf)
    val clusterListener = system.actorOf(Props[SimpleClusterListener], name = "clusterListener")
//    val proc = system.actorOf(Props[Processor], "processor")

    Cluster(system).subscribe(clusterListener, classOf[ClusterDomainEvent])

    def processors = Await.result(clusterListener ? LiveNodes, 5 seconds).asInstanceOf[Set[ActorSelection]]

    val minProcessors = ConfigFactory.load.getInt("minimum-processors")

    while(processors.size < minProcessors) { Thread.sleep(5000) }
    graph.findCommunities(processors)
    graph.printCommunities
  }
}

class SimpleClusterListener extends Actor with ActorLogging {

  val processors = Set.empty[ActorSelection]

  def receive = {
    case state: CurrentClusterState ⇒
      log.info("Current processors: {}", state.members.mkString(", "))

    case MemberUp(member) ⇒
      log.info("Member is Up: {}", member.address)
      if(member.hasRole("processor"))
        processors += context.actorSelection(RootActorPath(member.address) / "user" / "processor")

    case UnreachableMember(member) ⇒
      log.info("Member detected as unreachable: {}", member)

    case MemberRemoved(member, previousStatus) ⇒
      log.info("Member is Removed: {} after {}", member.address, previousStatus)
      processors -= context.actorSelection(RootActorPath(member.address) / "user" / "processor")

    case _: ClusterDomainEvent ⇒ // ignore

    case LiveNodes => sender ! processors
  }
}
