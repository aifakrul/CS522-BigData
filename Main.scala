import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

case class Iris(id: String, SLength:String,SWidth:String,PLength:String,PWidth:String,Species:String);

object Main {
  def main(args: Array[String]):Unit = {

    val conf = new SparkConf().setAppName("SparkScala").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN");


    val output = args(1)
    val  output_org=args(2);

    FileUtils.deleteDirectory(new File(output));
    FileUtils.deleteDirectory(new File(output_org));

    val csv=sc.textFile(args(0))

    //csv_input.foreach(f => println(f))
    val headerAndRows = csv.map(line => line.split(",").map(_.trim))
    val header = headerAndRows.first
    val mycsvdata = headerAndRows.filter(_(0) != header(0))
    val mydata = mycsvdata.map(p=>Iris(p(0),p(1),p(2),p(3),p(4),p(5)));
    val tmp1=mydata.map(x=>(x.Species,x.SLength.toDouble)).groupByKey().sortByKey();

    def compare_txt(str:String,cmp:String):Boolean =
    {
        str.equalsIgnoreCase("\""+cmp + "\"")
    }

    def get_mean_variance(x: RDD[Iris]) : RDD[(String, Double, Double)] ={
      val org_data=x.map(x=>(x.Species,x.SLength.toDouble)).groupByKey().sortByKey()
        .map(x => (x._1, (x._2.reduce(_+_)/x._2.size).toDouble, x._2))
        .map(x => (x._1, x._2, x._3.map(z => Math.pow(z - x._2, 2))))
        .map(x => (x._1,"%.2f".format(x._2).toDouble, "%.2f".format(x._3.reduce(_+_)/x._3.size).toDouble))
      org_data
    }

    val my_out_org=get_mean_variance(mydata);


    def makesample(n:Int): RDD[(String, Double, Double)]=
    {
      val s=mydata;//.filter(f => compare_txt(f.Species,species));

      val rs=(1 to n).map(x=> get_mean_variance(s.sample(true,1))).reduce(_ union _);
      val output = (rs.map(x => (x._1, x._2)).groupByKey().sortByKey()
        .map(x =>  (x._1, x._2.reduce(_+_)/x._2.size)))
      .join(rs.map(x => (x._1, x._3)).groupByKey()
        .map(x =>  (x._1, x._2.reduce(_+_).toDouble/x._2.size)))
        .map(x => (x._1, x._2._1, x._2._2))
      output
    }

    val res1=makesample(10);

    my_out_org.foreach(println);
    res1.map(f=> (f._1,"%.2f".format(f._2),"%.2f".format(f._3))).foreach(println);

    res1.coalesce(1).saveAsTextFile(output);
    my_out_org.coalesce(1).saveAsTextFile(output_org);

  }
}
