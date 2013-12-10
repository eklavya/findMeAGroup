object CommunityFinder extends App {
  val inputEdges = io.Source.fromFile("/home/eklavya/IdeaProjects/findMeAGroup/src/test/resources/graph").getLines.toList

  val numVertices = inputEdges.flatMap(_.split(",")).distinct.size

  val graph = {
    val graph = new Graph(numVertices)
    inputEdges.map { el =>
      val e = el.split(",")
      (e(0).toInt, e(1).toInt)
    } foreach(edge => graph.addEdge(edge._1, edge._2))
    graph
  }

  val res = graph.calcBetweenness

  for(i <- 0 until numVertices) {
    res(i).foreach { n =>
      println(s"Betweenness of edge $i, ${n.node} = ${n.betweenness}")
    }
  }
}