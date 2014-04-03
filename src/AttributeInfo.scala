package df
/*
 * only a structure used when choosing classification attribute
 */
class AttributeInfo(
  var gainRatio: Double,
  var attributeValues: Array[String],
  var indice: Int=0
  ) {

  def setIndice(indice: Int) {
    this.indice = indice
  }

}