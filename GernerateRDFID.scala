import java.io.InputStream
import java.util.Properties
import java.lang.Object._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import scala.collection.JavaConverters._
import scala.collection


/**
 * Created by gaoyanjie on 14-4-22.
 */
object loaddata {

  def loaddata() {

    //sc.textFile("data_final/geo_coordinates_en.nt").foreach(a=>println(a))
    val prop = new Properties();
    val in = getClass.getResourceAsStream("/loaddata.properties");
    prop.load(in);
    val paths = prop.getProperty("inputpath").trim()
    val master=prop.getProperty("master").trim
    val outputpath=prop.getProperty("outputpath").trim

    val sc = new SparkContext(master, "loaddata",
      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))

    //主1谓2宾0
    var zhuyuacc = sc.accumulator(0)
    var weiyuacc = sc.accumulator(0)
    var binyuacc = sc.accumulator(0)
    val pathsarray = paths.split("\\|")

    val allrdd=pathsarray.map(a=>sc.textFile(a)).foldLeft(sc.textFile(paths.split("\\|")(0)))((a,b)=>a.union(b))
    val zwbrdd = allrdd.flatMap(
     // .coalesce(32,true).
      line => {
        val str = line.split(" ")
        val ss = List((str(0), (1, 0)), (str(1), (2, 0)), (str(2), (0, 0)))
        ss
      }).reduceByKey((a, b) => {
      if (b._1 == 2) {

        (b._1, 0)
      }
      else {
        (b._1, a._1 + b._1)
      }
    }).map(a => {

      if (a._2._1 == 0 && a._2._2 != 0) {
        (a._1, 1)
      }
      else {

        if(a._1.contains("resource")){
          (a._1, 1)
        }
        else (a._1, a._2._1)
      }
    })

    zwbrdd.foreach(a => {

      if (a._2 == 2) weiyuacc += 1
      if (a._2 == 0) binyuacc += 1
      if (a._2 == 1) zhuyuacc += 1
    })


    val partitionnum = zwbrdd.mapPartitionsWithIndex {
      case (partitionIndex, iter) => {
        var zhuyu = 0
        var binyu = 0
        var weiyu = 0
        for (p <- iter) {
          if (p._2 == 2) weiyu += 1
          if (p._2 == 0) binyu += 1
          if (p._2 == 1) zhuyu += 1
        }
        (partitionIndex, (partitionIndex, zhuyu, weiyu, binyu)).productIterator
      }
    }.collect()

    val intpar = partitionnum.map(a => {


      var index = 1
      if (a.toString.length - 1 > 0) index = a.toString.length - 1
      val s = a.toString.substring(1, index).split(",")

      if (!s(0).equals("")&&s.length>3) {

        (s(0).toInt, s(1).toInt, s(2).toInt, s(3).toInt)
      }
      else null
    }).filter(a => a != null)

    val parmap = new java.util.HashMap[Int, par]

    intpar.sortBy(a => a._1)
    for (i <- 0 to intpar.length - 1) {
      val c = intpar(i)
      if (i == 0) {

        var pa = new par
        pa.parindex = c._1
        pa.zhuyustart = 0
        pa.zhuyuend = c._2 - 1
        pa.weiyustart = 0
        pa.weiyuend = c._3 - 1
        pa.binyustart = 0
        pa.binyuend = c._4 - 1
        parmap.put(i, pa)
      }
      else {
        var last = parmap.get(i - 1)

        last = last.asInstanceOf[par]
        var pa = new par
        pa.parindex = c._1
        pa.zhuyustart = last.zhuyuend + 1
        pa.zhuyuend = pa.zhuyustart + c._2-1
        pa.weiyustart = last.weiyuend + 1
        pa.weiyuend = pa.weiyustart + c._3-1
        pa.binyustart = last.binyuend + 1
        pa.binyuend = pa.binyustart + c._4-1
        parmap.put(i, pa)
      }
    }

    intpar.foreach(a => {

    })
    parmap.asScala.foreach(a => {

    })
    val qujianbro = sc.broadcast(parmap)
    zwbrdd.mapPartitionsWithIndex {
      case (partitionIndex, iter) => {
        val qujianbrov = qujianbro.value
        val splitqujian = qujianbrov.get(partitionIndex)

        val yufamapzhuyu = new collection.mutable.LinkedHashMap[String, (Int, Int)]

        var i1 = splitqujian.zhuyustart
        var i2 = splitqujian.weiyustart
        var i3 = splitqujian.binyustart

        iter.foreach(a=>{
          a._2 match {
            case 1  =>  { yufamapzhuyu.put(a._1, (i1, a._2))
              i1 += 1}
            case 2  =>  {  yufamapzhuyu.put(a._1, (i2, a._2))
              i2 += 1}
            case 0  =>  {  yufamapzhuyu.put(a._1, (i3, a._2))
              i3 += 1}
            case _  =>
          }

        })

        yufamapzhuyu.map(a => (a._1, a._2._1, a._2._2)).iterator
      }
    }.map(line => {
      (line._1+"  "+transnum.changeNum(line._2, line._3)+"  "+ bujiuzhuanhuan(line._3))

    }
      ).saveAsTextFile(outputpath)


    join.join(sc)
  }


  def  bujiuzhuanhuan(num:Int):Int=
  {
   num  match {
    case 1  =>  { 2 }    //主语
    case 2  =>  { 0 }      //谓语
    case 0  =>  { 1}     //宾语
    case _  =>    -1
  }
  }
  def main(args: Array[String]) {

    loaddata()


  }
}


class par  extends Serializable {
  var parindex :Int =0
  var zhuyustart :Int =0
  var zhuyuend :Int=0
  var weiyustart:Int =0
  var weiyuend :Int =0
  var binyustart:Int =0
  var binyuend:Int =0
}
