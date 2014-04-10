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
    val folder = new File(load_dir)
    if (!folder.exists()) {
      println(load_dir + " doesn't exist.")
      this
    } else {
      this.forest = Array[Elem]()

      for (i <- folder.listFiles()) {
        this.forest ++= Array(XML.load(i.getPath()))
      }
      this
    }
  }
  /*
   * save the trees in a folder 
   */
  def save(save_dir: String) {

    val folder = new File(save_dir)
    if (!folder.exists()) {
      folder.mkdir()
    }

    for (i <- folder.listFiles()) {
      i.delete
    }
    for (i <- 1 to forest.length) {
      XML.save(save_dir + "/tree" + i + ".xml", forest(i - 1))
    }

  }
  /*
   * the final decision is made by the ones with highest certainty in each group,
   * if the most convincing one in each group have the same certainty, 
   * it comes to the second convingcing one. 
   */
  def votingWithWeight(votes: Array[(String, Double)]): String = {

    var grouped = votes.groupBy(_._1).toArray.map(x => (x._1, x._2.map(_._2).sorted.reverse))
    var i = 0
    var result = ""
    while (grouped.length > 1) {
      val tmp = grouped
      grouped = grouped.filter(_._2.length > i)
      if (grouped.length > 0) {
        val m = grouped.map(x => x._2(i)).toArray.max
        grouped = grouped.filter(_._2(i) == m)
        i += 1
      } else {
        grouped = tmp.init
      }
    }
    grouped(0)._1

  }
  /*
 * core classify function which can deal with data point with missing value
 */
  def classify(features: Array[String], tree: Node): (String, Double) = {
    val nodetype = (tree \ "@type").toString

    if (nodetype == "L") {
      val decision = tree \ "decision"
      (decision.text, (decision \ "@p").toString.toDouble)
    } else {
      val value = features((tree \ "@indice").toString.toInt)
      val nodes = tree \ "node"

      if (nodetype == "N") {
        /*
         * numerical attribute
         */
        if (value == missValue) {
          val priorDecision = tree \ "priorDecision"
          var (decision, p) = classify(features, nodes.filter { x => (x \ "@branch").toString == priorDecision.text }(0))
          (decision, p * (priorDecision \ "@p").toString.toDouble)
        } else {
          if (value.toDouble > (tree \ "@splitPoint").toString.toDouble) {
            classify(features, nodes.filter { x => (x \ "@branch").toString == "high" }(0))
          } else {
            classify(features, nodes.filter { x => (x \ "@branch").toString == "low" }(0))
          }
        }
      } else {
        /*
         * categorical attribute
         */
        val node = nodes.filter { x => (x \ "@branch").toString == value }
        if (node.length == 0) {
          val priorDecision = tree \ "priorDecision"
          var (decision, p) = classify(features, nodes.filter { x => (x \ "@branch").toString == priorDecision.text }(0))
          (decision, p * (priorDecision \ "@p").toString.toDouble)
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
    votingWithWeight(votes)

  }

 
}