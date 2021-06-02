package sparkTest

object test01 {
  def main(args: Array[String]): Unit = {
    val names = List("1 2 3 4")
    names.flatMap(x=>x.split(",")).foreach(x=>print(x))
    print("hahah")


//    val strings = new util.ArrayList[String]()
//    strings.add("aa")
//    strings.add("bb")
//    println(strings.get(0))
//    val list = List(1, 2, 3, 4)
//    val ints: List[Int] = list.map(x=>x+1)
    val tuples: List[(Int, Int)] = List((1,2), (3,4), (5,6))
    val tuples02: List[(Int, Int)] = tuples.map{case (x,y) =>{(x+1,y)}}
    tuples02.foreach(x=>print("----"+x._1+":"+x._2+" "))
//    val ints02: List[Int] = tuples.map(x=>x._2)
//    ints02.foreach(x=>println(x))
  }
}
