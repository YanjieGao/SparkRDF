/**
 * Created by gaoyanjie on 14-4-28.
 */
object transnum {

  def main(args:Array[String])
  {


  }

  def changeNum(num:Long,flag:Int):Long={
    //主谓宾     由第63,62位区分主谓宾，主语10宾语01谓语00
    //主1谓2宾0
    flag match {
      case 0 => toBinyu(num)
      case 1 => toZhuyu(num)
      case 2 =>  num
      case _ =>  -1
  }    }
  def  toZhuyu(num:Long):Long={
    //因为是带符号数，所以就左移动62位最高位当符号位用
  // val temp=(1L<<62)^((1L<<24)-1)
    val temp=(1L<<62)
    //(num+1L<<23-1)^temp
    num^temp
  }

  def toBinyu(num:Long):Long={
    //   val temp=((1L<<24)-1)
    val temp=(1L<<61)
     // (num+1L<<23-1)^temp
    num^temp
  }

  //谓语不需要转换占用低24位即可满足
  def toWeiyu():Long={
      1
  }

}
