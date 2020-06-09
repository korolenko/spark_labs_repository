import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer,StopWordsRemover}
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.functions.{udf,col}
val idval = "23325"
val resultfile: String = "user/roman.korolenko/lab02"
val rf = resultfile+"_"+ idval

val path = "/labs/laba02/DO_record_per_line.json"
val sentenceData = spark.read.json(path).orderBy("id")

import org.apache.spark.sql.functions.{ expr, udf, col }
def cleanString: String => String = s => s.replaceAll("[^A-Za-zÑñА-яа-я0-9 ]", "")
def cleanStringUdf = udf(cleanString)
val dfID = sentenceData.filter("id = "+idval)
val coursesDF = dfID.join(sentenceData.as("dft"),Seq("lang"),"inner").select("dft.*")
val cleanedCourserDF = coursesDF.withColumn("words",cleanStringUdf(col("desc")))

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.functions.{udf,col}
val tokenizer = new Tokenizer().setInputCol("desc").setOutputCol("words")
val wordsData = tokenizer.transform(sentenceData)

val stopWordsRemover = new StopWordsRemover().setInputCol("words").setOutputCol("cleanWords")
val stopWordsRemovedCoursesDf = stopWordsRemover.transform(wordsData)

val hashingTF = new HashingTF()
  .setInputCol("cleanWords").setOutputCol("rawFeatures").setNumFeatures(10000)

val featurizedData = hashingTF.transform(stopWordsRemovedCoursesDf)

val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
val idfModel = idf.fit(featurizedData)

val rescaledData = idfModel.transform(featurizedData)

val normalizer = new Normalizer().setInputCol("features").setOutputCol("normFeatures")
val normalizedData = normalizer.transform(rescaledData)

normalizedData.select("id", "normFeatures").show(3)

val left = normalizedData.filter("id = "+ idval).withColumnRenamed("normFeatures", "curFeature")

def scalarCos: (SparseVector,SparseVector) => Double = {(x,y) =>
    val v1 = x.toDense.toArray
    val v2 = y.toDense.toArray
    var mult = 0.0
    var i = 0
    v1.foreach{x=>
      mult += v1(i) * v2(i)
      i +=1
    }
    mult
}
def udf_scalar = udf(scalarCos)

left.crossJoin(normalizedData)
    .drop(left.col("id"))
    .withColumn("cosine", udf_scalar(col("curFeature"), col("normFeatures")))
    .select("id", "cosine").orderBy(col("cosine").desc).limit(11).show