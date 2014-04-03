package df

import org.apache.spark.rdd.RDD
import scala.xml._
import scala.xml.Node
import java.io.File
class RandomForestModel(
  var forest: Array[Elem],
  var missValue: String)
  extends Serializable {
  def this() = { this(Array[Elem](), "?") }
  def loadForest(load_dir: String) :RandomForestModel= {

    this.forest = Array[Elem]()
    val files = new File(load_dir)
    for (i <- files.list()) {
      this.forest ++= Array(XML.load(load_dir + "/" + i))
    }
    this
  }
  def majority(buf: Array[String]): String = {
    var major = buf(0)
    var majorityNum = 0
    for (i <- buf.distinct) {
      val tmp = buf.count(x => x == i)
      if (tmp > majorityNum) {
        majorityNum = tmp
        major = i
      }
    }
    major
  }

  def classify(features: Array[String], tree: scala.xml.Node): String = {
    if ((tree \ "type").text == "L") {
      (tree \ "result").text
    } else {
      val value = features((tree \ "indice").text.toInt)
      val nodes = tree \ "node"

      if ((tree \ "type").text == "N") {
        if (value == missValue) {
          classify(features, nodes.filter { x => (x \ "@branch").toString == (tree \ "priorDecision").text }(0))
        } else {
          if (value.toDouble > (tree \ "splitPoint").text.toDouble) {
            classify(features, nodes.filter { x => (x \ "@branch").toString == "high" }(0))
          } else {
            classify(features, nodes.filter { x => (x \ "@branch").toString == "low" }(0))
          }
        }
      } else {
        val node = nodes.filter { x => (x \ "@branch").toString == value }
        if (node.length == 0) {
          classify(features, nodes.filter { x => (x \ "@branch").toString == (tree \ "priorDecision").text }(0))
        } else {
          classify(features, node(0))
        }
      }

    }

  }
  def predict(features: Array[String]): String = {

    val votes = for (tree <- forest) yield {
      classify(features, tree)
    }
    majority(votes)

  }
  def save(save_dir: String) {

    for (i <- 1 to forest.length) {
      XML.save(save_dir + "/tree" + i + ".xml", forest(i - 1))
    }
  }

}