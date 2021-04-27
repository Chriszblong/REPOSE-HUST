package AuxiFunPackage
class MyPair[A:Numeric,B](var dist:A,var label:B){}
class MutableMyPair[A<:Double,B](val a:A,val b:B)extends Ordered[MutableMyPair[A,B]]{
  override def compare(that: MutableMyPair[A, B]): Int = {
    a.compareTo(that.a)
  }
}

case class Result(result:Array[Array[MutableMyPair[Double,Int]]] ){

}
