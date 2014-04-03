package df
/*
 * each line of the data file is an instance
 */
case class Instance(label: String, features: Array[String]) {
  override def toString: String = {
    "Observation(%s, %s)".format(label, features.mkString("[", ", ", "]"))
  }

  
}