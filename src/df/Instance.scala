package df

case class Instance(label: String, features: Array[String]) {
  override def toString: String = {
    "Observation(%s, %s)".format(label, features.mkString("[", ", ", "]"))
  }

  
}