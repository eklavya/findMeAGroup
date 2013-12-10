import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by eklavya on 4/12/13.
 */

/*
a) breadth first search can find shortest paths from a vertex s to all others

1. The initial vertex s is given distance ds = 0 and a weight ws = 1.
2. Every vertex i adjacent to s is given distance di = ds + 1 = 1, and weight wi = ws = 1.
3. For each vertex j adjacent to one of those vertices i we do one of three things:
  (a) If j has not yet been assigned a distance, it is assigned distance dj = di + 1 and weight wj = wi .
  (b) If j has already been assigned a distance and dj = di + 1, then the vertex’s weight is increased by wi,
      that is wj ← wj + wi .
  (c) If j has already been assigned a distance and dj < di + 1, we do nothing.
4. Repeat from step 3 until no vertices remain that have assigned distances but whose neighbors do not
   have assigned distances.


b)calculate edge betweenness contribution from this set of paths

1. Find every “leaf” vertex t, i.e., a vertex such that no paths from s to other vertices go though t.
2. For each vertex i neighboring t assign a score to the edge from t to i of wi/wt.
3. Now, starting with the edges that are farthest from the source vertex s lower down in a diagram such
   as Fig. 4b — work up towards s. To the edge from vertex i to vertex j, with j being farther from s
   than i, assign a score that is 1 plus the sum of the scores on the neighboring edges immediately
   below it (i.e., those with which it shares a common vertex), all multiplied by wi/wj.
4. Repeat from step 3 until vertex s is reached.

 */

case class Neighbour(node: Int, var betweenness: Double)

class Graph(numVertices: Int) {

  private[this] val graph = Array.fill[List[Neighbour]](numVertices)(List[Neighbour]())

  private[this] val acc = Array.fill[List[Neighbour]](numVertices)(List[Neighbour]())

  def getEdges(v: Int) = graph(v)

  def addEdge(v1: Int, v2: Int) {
    graph(v1) = Neighbour(v2, 0.0) :: graph(v1)//Edge(v2, 0.0) :: graph(v1)
    acc(v1) = Neighbour(v2, 0.0) :: acc(v1)
  }

  /*
  When we arrive from a node i at a node j which has already been visited and distance(i) + 1 == distance(j)
   it means that i provides another shortest path to this node and we add it to a list shortestPathNodeList

   Note: We can get away with just a listing of all the nodes which fall on any shortest path to a node
         because we only need to know if they fall on a shortest path of any node to decide whether they are a leaf
         or not, we do not use shortest paths for any other purpose.
   */
  def calcShortestPathTree(source: Int, distance: Array[Int], weights: Array[Int], arrivedFrom: Array[Int]) = {
    val bfsq        = mutable.Queue.empty[Int]
    val shortestPathNodeList = Array.fill[List[Int]](numVertices)(List[Int]())

    @tailrec
    def bfsTraverse {
      if (! bfsq.isEmpty) {
        val node = bfsq.dequeue
        val neighbours = graph(node)
        neighbours.foreach { n =>
          val j = n.node
          if (distance(j) == -1) {
            bfsq.enqueue(j)
            arrivedFrom(j) = node
            shortestPathNodeList(j) = node :: shortestPathNodeList(j)
            distance(j) = distance(node) + 1
            weights(j)  = weights(node)
          }
          else if (distance(j) < distance(node) + 1) ()
          else if (distance(j) == distance(node) + 1) {
            shortestPathNodeList(j) = node :: shortestPathNodeList(j)
            weights(n.node) += weights(node)
          }
        }
        bfsTraverse
      }
    }

    bfsq.enqueue(source)
    bfsTraverse

    shortestPathNodeList
  }



  def calcLeaves(shortestPathNodeList: Array[List[Int]]) = {
    val leaves = new ArrayBuffer[Int]

    shortestPathNodeList.indices foreach { i =>
      if (shortestPathNodeList.filter(_.contains(i)).isEmpty)
        leaves += i
    }

    leaves
  }

  def calcBetweenness = {

    (0 until numVertices) foreach { s =>
      val distance             = Array.fill[Int](numVertices)(-1)
      val weights              = new Array[Int](numVertices)
      val arrivedFrom          = new Array[Int](numVertices)

      distance(s) = 0
      weights(s)  = 1

      /*
      Find shortest path to all vertices from s
       */
      val shortestPathNodeList = calcShortestPathTree(s, distance, weights, arrivedFrom)

      /*
      get leaves
       */
      val leaves = calcLeaves(shortestPathNodeList)

      /*
      assign edge betweenness scores to all the edges comprising of the leaves
       */
      leaves foreach { leaf =>
        val nbrs = graph(leaf)
        nbrs foreach { n =>
          n.betweenness = weights(n.node).toDouble / weights(leaf).toDouble
          graph(n.node).filter(_.node == leaf).head.betweenness = n.betweenness
        }
      }

      /*
      from this node walk up to s and modify betweenness for edge between l and the node j (closer to s), assign a score
      that is 1 plus the sum of the scores on the neighboring edges immediately below it (farther from s),
      all multiplied by (weight of l)/(weight of j)
       */
      def addBetweenness(l: Int) {
        if (l != s) {
          val target = graph(l).filter(e => distance(e.node) < distance(l)).head
          if (target.node != s) {
            target.betweenness = (1.0 + graph(l).filter(e => distance(e.node) > distance(l)).foldRight(0.0)(_.betweenness + _)) *
                (weights(l) / weights(target.node))
            graph(target.node).filter(_.node == l).head.betweenness = target.betweenness
            addBetweenness(target.node)
          }
        }
      }

      /*
      walk up from all the neighbours of leaves
       */
      leaves foreach { leaf =>
        val nbrs = graph(leaf)
        nbrs foreach { n =>
          addBetweenness(n.node)
        }
      }

    /*
    Add contribution to edge betweenness from this source in the accumulator
     */
      (acc zip graph) foreach { case(ael, el) =>
        (ael zip el) foreach { case (e1, e2) =>
          e1.betweenness += e2.betweenness
        }
      }

    /*
    Reset graph for next calculation with a different node as the source.
     */
      graph map (el => el map (n => n.betweenness = 0.0))

    }

    acc
  }
}
