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
    //missing value  
  val missValue: String,
  //the name of features
  val featureName: Array[String],
  //whether the feature is numerical
  val featureIsNumerical: Array[Boolean],
  //the number of trees to build per worker
  val numTree: Int,
  //the number of dimensions of the features
  val M: Int,
  //the number of features for each tree
  val m: Int)
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
   * bootstrap sampling (out of bag)
   */
  def bagging(data: Array[Instance]): Array[Instance] = {

    val len = data.length

    var result = data
    for (i <- 0 until len) {
      result(i) = data(Random.nextInt(len))
    }
    result
  }
/*
 * calculate the entropy gain ratio of a numerical attribute 
 */
  private def gainRatioNumerical(att_cat: Array[(String, String)], ent_cat: Double): AttributeInfo = {
    val sorted = att_cat.map(line => (line._1.toDouble, line._2)).sortBy(line => line._1)
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
    val att = att_cat.map(obs => obs._1)
    val values = att.distinct
    if (values.length != 1) {
      var gain = ent_cat
      val invL = 1.0 / att_cat.length
      for (i <- values) {
        val cat_i = att_cat.filter(obs => obs._1 == i).map(obs => obs._2)
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
 * the core function to grow an un-pruning tree
 */
  def growTree(data: Array[Instance], featureIndice: Array[Int], branch: String = ""): Elem = {

    var attList = Set[AttributeInfo]()
    val cat = data.map(obs => obs.label)
    val ent_cat = entropy(cat)

    if (ent_cat == 0) {
      <node branch={ branch }>
        <type>L</type>
        <result>{ data(0).label }</result>
      </node>
    } else {
      for (i <- featureIndice) {
        val att_cat = data.filter(obs => obs.features(i) != missValue)
          .map(obs => (obs.features(i), obs.label))
        if (att_cat.length > 0) {
          if (featureIsNumerical(i)) {
            val attInfo = gainRatioNumerical(att_cat, ent_cat)
            attInfo.setIndice(i)
            attList ++= Set(attInfo)
          } else {
            val attInfo = gainRatioCategorical(att_cat, ent_cat)
            attInfo.setIndice(i)
            attList ++= Set(attInfo)

          }
        }
      }
      /*
       * find the best attribute for classification
       */
      val chosen = attList.maxBy(_.gainRatio)
      if (chosen.gainRatio == 0) {
        <node branch={ branch }>
          <type>L</type>
          <result>{ majority(data.map(x => x.label)) }</result>
        </node>

      } else {
        if (featureIsNumerical(chosen.indice)) {//numerical attribute
          val splitPoint = chosen.attributeValues(0).toDouble
          val lochilddata = data.filter { obs =>
            val j = obs.features(chosen.indice)
            j != missValue && j.toDouble <= splitPoint
          }

          val hichilddata = data.filter { obs =>
            val j = obs.features(chosen.indice)
            j != missValue && j.toDouble > splitPoint
          }

          <node branch={ branch }>
            <name>{ featureName(chosen.indice) }</name>
            <indice>{ chosen.indice }</indice>
            <type>N</type>
            <splitPoint>{ splitPoint }</splitPoint>
            { growTree(lochilddata, featureIndice, "low") }
            { growTree(hichilddata, featureIndice, "high") }
            <priorDecision>{
              if (lochilddata.length >= hichilddata.length) "low" else "high"
            }</priorDecision>
          </node>

        } else {//categorical attribute

          var priorDecision = ""
          var majorityNum = 0

          <node branch={ branch }>
          	<name>{ featureName(chosen.indice) }</name>
          	<indice>{ chosen.indice }</indice>
            <type>C</type>
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
                growTree(child, featureIndice, i)
              }
            }
            <priorDecision>{ priorDecision }</priorDecision>
          </node>

        }
      }

    }

  }

  def run(data: RDD[Instance]): RandomForestModel = {
    var M = data.first.features.length
    var total_len = data.count

    var indieces = List
    /*
     * it's like Mahout decision forest which builds a random forest using partial data. 
     * Each worker uses only the data given by its partition.
     */
    var forest = data.mapPartitionsWithIndex { (index, obs) =>

      val partial = obs.toArray
      val treesPerWorker = for (i <- 1 to numTree) yield {
        val oob = bagging(partial)
        val featureIndice = randomIndices(m, M)

        println("Tree:" + i + " vol:" + oob.length)

        growTree(oob, featureIndice)
        
      }
      treesPerWorker.iterator
    }.collect

    new RandomForestModel(forest, missValue)
  }
}

object RandomForest {

  def train(
    data: RDD[Instance],
    info:Elem,
    missValue: String,
    m: Int,
    numTree: Int): RandomForestModel =
    {
      val M = data.first.features.length
      var featureName = Array[String]()
      var featureIsNumerical = Array[Boolean]()
      /*
       * read the XML
       */
      for (att <- info\"attribute") {
        featureName++=Array(att.text)
        if((att\"@type").toString=="C"){
          featureIsNumerical++=Array(false)
        }else{
          featureIsNumerical++=Array(true)
        }
      }
      val rf = new RandomForest(missValue, featureName, featureIsNumerical, numTree, M, m)
      rf.run(data)
    }

  /*
   * Main function
   * The attribute description should be given by info_dir.
   * We can save the random forest in a XML file named save_dir.
   */
  def main(args: Array[String]) {
    if (args.length != 4) {
      println("Usage: RandomForest <master> <input_dir> <info_dir> <save_dir>")
      System.exit(1)
    }
    val (master, inputFile, infoFile, save_dir) = (args(0), args(1), args(2), args(3))

    val sc = new SparkContext(master, "RandomForest")
    val data = sc.textFile(inputFile).map { line =>
      val parts = line.split(',')
      Instance(parts.last, parts.init)
    }.cache()
    val info=XML.load(infoFile)
    val missValue="?"
    val model = RandomForest.train(data, info, missValue, 26, 3)

    model.save(save_dir)
    //val model=new RandomForestModel().loadForest(save_dir)
    val labelAndPreds = data.map { obs =>
      val prediction = model.predict(obs.features)
      (obs.label, prediction)
    }
    val trainErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / labelAndPreds.count
    println("Training Error=" + trainErr)

  }
}


  

