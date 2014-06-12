import java.util.Properties
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel

/**
 * Created by gaoyanjie on 14-4-29.
 */
object join {
  def main(args:Array[String]) {

  }

  def join(sc:SparkContext){

    val prop = new Properties();
    val in = getClass.getResourceAsStream("/loaddata.properties");
    prop.load(in);
    val master=prop.getProperty("master").trim
    val outputpath=prop.getProperty("outputpath").trim
    val inputpath=prop.getProperty("inputpath").trim
    val outputjoinpath=prop.getProperty("outputjoinpath").trim

    val rdd=sc.textFile(outputpath)
     val  rddmap= rdd.map(a=>{
      val s:Array[String]=a.split(" ") ;
      (s(0),s(2))
    }
    )
     val pathsarray = inputpath.split("\\|")
    val data=pathsarray.map(a=>sc.textFile(a)).foldLeft(sc.textFile(inputpath.split("\\|")(0)))((a,b)=>a.union(b))
    val datamapzhuyu =data.map(a=>{val s:Array[String]=a.split(" ") ;(s(0),s(1)+"  "+s(2))})
    val joinzhuyu= datamapzhuyu.join(rddmap)
   val  joinweiyu=joinzhuyu.map(a=>{
      val split=a._2._1.split(" ")
   (split(0),split(2)+" "+a._2._2)
    }).join(rddmap)

    val joinbinyu=joinweiyu.map(a=>{
     val split=a._2._1.split(" ")
      (split(0),split(1)+" "+a._2._2)
    }).join(rddmap)
    rddmap.unpersist()
    joinbinyu.map(a=>{
 val last=a._2._1+" "+a._2._2
      last
    }).saveAsTextFile(outputjoinpath)
 }
}

