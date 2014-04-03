spark.mllib.RandomForest
========================
Random Forest is one of the most famous algorithms in Data Mining and Machine Learning. 
This package is based on Spark and implemented in scala. The main function is in RandomForest.scala.

We use XML file to store the decision tree model and describe the attribute (name and type).
A random forest is consist of several un-pruning decision trees. Every tree can deal with 
categorical and numerical attribute.It can also deal with data with missing values.

The XML file describing the attributes is like follows:
<info>
<attribute type="N">age</attribute>
<attribute type="C">sex</attribute>
<attribute type="C">psych</attribute>
</info>
