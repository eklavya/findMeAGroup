object CommunityFinder extends App {
  var inputEdges = Map.empty[String, String]

  io.Source.fromFile("/home/eklavya/followersData.csv").getLines.toList.foreach{ s =>
    val pairs = s.split(",")
    val k = pairs(0)
    val v = pairs(1)
    inputEdges = inputEdges + (k -> v)
  }

  var temp = Set.empty[String]

  inputEdges.foreach {
    case(k, v) =>
      inputEdges.get(v).map( x => if (x == k)  temp += x)
  }

  inputEdges = inputEdges.filterNot{case(k, v) => temp(k)}

  def vertexMapper(v: String) = v.hashCode
}