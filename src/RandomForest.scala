package df

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.util.Random
import scala.xml._
import scala.io.Source
/*
 * Random Forest 
 * The core Map-Reduce method is very similar to df (decision forest) in Mahout
 * We use XML to store decision trees.
 */
class RandomForest(
  /*
   * missing value
   */
  val missValue: String,
  /*
   * the name of features
   */
  val featureName: Array[String],
  /*
   * whether the feature is numerical
   */
  val featureIsNumerical: Array[Boolean],
  /*
   * the number of trees to build per worker 
   */
  val numTree: Int,
  /*
   * the number of dimensions of the features
   */
  val M: Int,
  /*
   * the number of features for each tree
   */
  val m: Int,
  /*
   * m selected features' indices 
   */
  var featureIndice: Array[Int] = null)
  extends Serializable {
  /*
   * select m numbers in [0,M)
   */
  private def randomIndices(m: Int, M: Int): Array[Int] = {
    if (m < M && m > 0) {
      var result = Array.fill(m)(0)
      var i = 0

      while (i < m) {
        val n = Random.nextInt(M)
        if (!result.contains(n)) {
          result(i) = n
          i += 1
        }
      }
      result
    } else {
      var result = Array.fill(M)(0)
      for (i <- 0 until M) {
        result(i) = i
      }
      result
    }
  }
  /*
   * calculate the entropy of an array
   */
  private def entropy(buf: Array[String]): Double = {
    val len = buf.length
    val invLog2 = 1.0 / Math.log(2)
    if (len > 1) {
      val invLen = 1.0 / len.toDouble
      var ent = 0.0

      for (v <- buf.distinct) {
        val p_v = buf.count(x => x == v).toDouble * invLen
        ent -= p_v * Math.log(p_v) * invLog2
      }
      ent

    } else {
      0.0
    }
  }

  /*
   * bootstrap sampling and select m features
   */
  def baggingAndSelectFeatures(data: Array[Instance]): Array[Instance] = {

    val len = data.length

    val result = for (i <- 0 until len) yield {
      val point = data(Random.nextInt(len))
      var selectedFeatures = Array.fill(m)("")
      for (j <- 0 until m) {
        selectedFeatures(j) = point.features(featureIndice(j))
      }
      Instance(point.label, selectedFeatures)
    }
    result.toArray
  }
  /*
 * calculate the entropy gain ratio of a numerical attribute 
 */
  private def gainRatioNumerical(att_cat: Array[(String, String)], ent_cat: Double): AttributeInfo = {
    val sorted = att_cat.map(line => (line._1.toDouble, line._2)).sortBy(_._1)
    val catValues = sorted.map(line => line._2)
    val attValues = sorted.map(line => line._1)
    if (attValues.distinct.length == 1) {
      new AttributeInfo(0, Array(""))
    } else {
      val invLog2 = 1.0 / math.log(2)
      val len = catValues.length
      val invLen = 1.0 / len.toDouble
      var maxInfoGain = 0.0
      var c = 1
      for (i <- 1 until len) {

        if (catValues(i - 1) != catValues(i)) {

          var infoGain = ent_cat
          infoGain -= i * invLen * entropy(catValues.take(i))
          infoGain -= (1 - i * invLen) * entropy(catValues.takeRight(len - i))

          if (infoGain > maxInfoGain) {
            maxInfoGain = infoGain
            c = i
          }
        }
      }
      if (attValues(c) == attValues.last) {
        new AttributeInfo(0, Array(""))
      } else {
        val p = c * invLen
        val ent_att = -p * math.log(p) * invLog2 - (1 - p) * math.log(1 - p) * invLog2
        val infoGainRatio = maxInfoGain / ent_att
        val splitPoint = (attValues(c - 1) + attValues(c)) * 0.5
        new AttributeInfo(infoGainRatio, Array(splitPoint.toString))
      }
    }
  }
  /*
 * calculate the entropy gain ratio of a categorical attribute 
 */
  private def gainRatioCategorical(att_cat: Array[(String, String)], ent_cat: Double): AttributeInfo = {
    val att = att_cat.map(_._1)
    val values = att.distinct
    if (values.length != 1) {
      var gain = ent_cat
      val invL = 1.0 / att_cat.length
      for (i <- values) {
        val cat_i = att_cat.filter(_._1 == i).map(_._2)
        gain -= cat_i.length * invL * entropy(cat_i)
      }

      val gainRatio = gain / entropy(att)
      new AttributeInfo(gainRatio, values)
    } else {
      new AttributeInfo(0, values)
    }
  }
  /*
 * calculate the majority in an array
 */
  def majority(buf: Array[String]): (String, Double) = {
    var major = buf(0)
    var majorityNum = 0
    for (i <- buf.distinct) {
      val tmp = buf.count(x => x == i)
      if (tmp > majorityNum) {
        majorityNum = tmp
        major = i
      }
    }
    (major, majorityNum.toDouble / buf.length)
  }
  /*
 * the core function to grow an un-pruning tree
 */
  def growTree(data: Array[Instance], branch: String = ""): Elem = {

    var attList = List[AttributeInfo]()
    val cat = data.map(_.label)
    val ent_cat = entropy(cat)

    if (ent_cat == 0) {
      <node branch={ branch } type="L">
        <decision p="1">{ data(0).label }</decision>
      </node>
    } else {
      for (i <- 0 until m) {
        val att_cat = data.filter(_.features(i) != missValue)
          .map(obs => (obs.features(i), obs.label))
        if (att_cat.length > 0) {
          if (featureIsNumerical(featureIndice(i))) {
            val attInfo = gainRatioNumerical(att_cat, ent_cat)
            attInfo.setIndice(i)
            attList ::= attInfo
          } else {
            val attInfo = gainRatioCategorical(att_cat, ent_cat)
            attInfo.setIndice(i)
            attList ::= attInfo

          }
        }
      }
      /*
       * find the best attribute for classification
       */
      val chosen = attList.maxBy(_.gainRatio)
      if (chosen.gainRatio == 0) {
        val (decision, p) = majority(data.map(_.label))
        <node branch={ branch } type="L">
          <decision p={ p.formatted("%.3f") }>{ decision }</decision>
        </node>

      } else {
        var priorDecision = ""
        val indice = featureIndice(chosen.indice)
        if (featureIsNumerical(indice)) {
          /*
           * numerical attribute
           */
          val splitPoint = chosen.attributeValues(0).toDouble
          val lochilddata = data.filter { obs =>
            val j = obs.features(chosen.indice)
            j != missValue && j.toDouble <= splitPoint
          }

          val hichilddata = data.filter { obs =>
            val j = obs.features(chosen.indice)
            j != missValue && j.toDouble > splitPoint
          }
          var p = lochilddata.length.toDouble / data.length
          if (p <= 0.5) {
            p = 1 - p
            priorDecision = "high"
          } else {
            priorDecision = "low"
          }
          <node branch={ branch } 
                type="N" 
                name={ featureName(indice) } 
                indice={ indice.toString } 
                splitPoint={ splitPoint.formatted("%.2e") }>
            { growTree(lochilddata, "low") }
            { growTree(hichilddata, "high") }
            <priorDecision p={ p.formatted("%.3f") }>{
              priorDecision
            }</priorDecision>
          </node>

        } else {
          /*
           * categorical attribute
           */
          var majorityNum = 0

          <node branch={ branch } 
                type="C" 
                name={ featureName(indice) } 
                indice={ indice.toString }>
            {
              for (i <- chosen.attributeValues) yield {
                val child = data.filter { obs =>
                  val j = obs.features(chosen.indice)
                  j != missValue && j == i
                }
                if (child.length > majorityNum) {
                  majorityNum = child.length
                  priorDecision = i
                }
                growTree(child, i)
              }
            }
            <priorDecision p={ (majorityNum.toDouble / data.length).formatted("%.3f") }>{
              priorDecision
            }</priorDecision>
          </node>

        }
      }

    }

  }
  /*
   * it's very similar to Mahout decision forest which builds a random forest using partial data. 
   * Each worker uses only the data given by its partition.
   */
  def run(data: RDD[Instance]): RandomForestModel = {

    var total_len = data.count
    var forest = data.mapPartitionsWithIndex { (index, obs) =>
      val partial = obs.toArray
      val treesPerWorker = for (i <- 1 to numTree) yield {
        featureIndice = randomIndices(m, M)
        val oob = baggingAndSelectFeatures(partial)
        //println("Tree:" + i + " vol:" + oob.length)
        growTree(oob)

      }
      treesPerWorker.iterator
    }.collect

    new RandomForestModel(forest, missValue)
  }
}

object RandomForest {

  def train(
    data: RDD[Instance],
    info: Elem,
    missValue: String,
    m: Int,
    numTree: Int): RandomForestModel =
    {
      /*
       * read the XML
       */
      var featureName = Array[String]()
      var featureIsNumerical = Array[Boolean]()
      for (att <- info \ "attribute") {
        featureName ++= Array(att.text)
        if ((att \ "@type").toString == "C") {
          featureIsNumerical ++= Array(false)
        } else {
          featureIsNumerical ++= Array(true)
        }
      }
      val M = featureIsNumerical.length
      if (m < M) {
        new RandomForest(missValue, featureName, featureIsNumerical, numTree, M, m).run(data)
      } else {
        new RandomForest(missValue, featureName, featureIsNumerical, numTree, M, M).run(data)
      }
    }

  /*
   * Main function
   * The attribute description should be given by info_dir.
   * We can save the random forest in a XML file named save_dir.
   */
  def main(args: Array[String]) {
    if (args.length != 6) {
      println("Usage: RandomForest <master> <input_dir> <info_dir> <save_dir> <m> <treesPerWorker>")
      System.exit(1)
    }
    val (master, inputFile, infoFile, save_dir, m, treesPerWorker) = (args(0), args(1), args(2), args(3), args(4).toInt, args(5).toInt)

    val sc = new SparkContext(master, "RandomForest")
    val data = sc.textFile(inputFile).map { line =>
      val parts = line.split(',')
      Instance(parts.last, parts.init)
    }.cache()

    val info = XML.load(infoFile)
    val missValue = "?"
    var model = RandomForest.train(data, info, missValue, m, treesPerWorker)
    model.save(save_dir)
    model = new RandomForestModel().loadForest(save_dir)
    val trainErr = data.filter { obs =>
      val pred = model.predict(obs.features)
        pred != obs.label
      }.count.toDouble / data.count

    println("Training Error=" + err.sum / err.length)

  }
}


  

