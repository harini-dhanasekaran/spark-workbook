package haruk

import org.apache.spark._
import org.apache.log4j._

object customer_purchase{
  def parseLine(line :String):(Int, Float) ={
    val cols = line.split(",")
    val customer_id = cols(0).toInt
    val amount = cols(2).toFloat
    (customer_id, amount)
  }
  def main(arg: Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext(master = "local[*]", appName = "customer_purchase")
    val ds = sc.textFile(path = "data/customer-orders.csv")
    val data = ds.map(parseLine)
    val amountSpentPerCustomer = data.reduceByKey((x,y) => x+y)
    val flip = amountSpentPerCustomer.map(x => (x._2, x._1))
    val temp = flip.sortByKey()
    val ans = temp.collect()
    ans.foreach(println)

  }
}