package AuxiFunPackage
class MyTimer {
  private var time:Long=0
  def restart()={
    time=System.currentTimeMillis()
  }
  def elapsed() ={
    System.currentTimeMillis()-time
  }
}

