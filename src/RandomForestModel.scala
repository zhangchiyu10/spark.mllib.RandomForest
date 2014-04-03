package df

import org.apache.spark.rdd.RDD
import scala.xml._
import java.io.File
class RandomForestModel(
  var forest: Array[Elem],
  var missValue: String)
  extends Serializable {
  def this() = { this(Array[Elem](), "?") }
  /*
   * load a forest from a folder with XML files
   */
  def loadForest(load_dir: String): RandomForestModel = {

    this.forest = Array[Elem]()
    val files = new File(load_dir)
    for (i <- files.list()) {
      this.forest ++= Array(XML.load(load_dir + "/" + i))
    }
    this
  }
  /*
   * save the trees in a folder 
   */
  def save(save_dir: String) {

    for (i <- 1 to forest.length) {
      XML.save(save_dir + "/tree" + i + ".xml", forest(i - 1))
    }
  }
  /*
   * the final decision is by majority
   */
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
  /*
 * core classify function which can deal with data point with missing value
 */
  def classify(features: Array[String], tree: Node): String = {
    if ((tree \ "type").text == "L") {
      (tree \ "result").text
    } else {
      val value = features((tree \ "indice").text.toInt)
      val nodes = tree \ "node"

      if ((tree \ "type").text == "N") { //numerical attribute
        if (value == missValue) {
          classify(features, nodes.filter { x => (x \ "@branch").toString == (tree \ "priorDecision").text }(0))
        } else {
          if (value.toDouble > (tree \ "splitPoint").text.toDouble) {
            classify(features, nodes.filter { x => (x \ "@branch").toString == "high" }(0))
          } else {
            classify(features, nodes.filter { x => (x \ "@branch").toString == "low" }(0))
          }
        }
      } else { //categorical attribute
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

}