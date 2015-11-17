package inputdata

import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD

/**
  * Created by reasono on 15/11/17.
  */
class MofunContentDataHolder(dataDirectoryPath: String) extends DataHolder with Serializable {

  protected val ratings = loadRatingsFromADirectory()
  protected val productsIDsToNameMap = loadIDsToProductnameMapFromADirectory(dataDirectoryPath)

  protected def loadRatingsFromADirectory(): RDD[Rating] = {

    val ratings = spark.sparkEnvironment.sc.textFile(dataDirectoryPath + "/base_data/*/*").map { line =>
      val fields = line.split("::")
      // format: Rating(userID, movieID, rating)
      try {
        Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
      } catch {
        case e: Exception => Rating(0, 0, 0.0)
      }
    }

    ratings.filter(r => productsIDsToNameMap.contains(r.product))
  }

  protected def loadIDsToProductnameMapFromADirectory(dataDirectoryPath: String): Map[Int, String] = {
    val movies = spark.sparkEnvironment.sc.textFile(dataDirectoryPath + "/mysql_data/user/*/*").map { line =>
      val fields = line.split(":")
      // format: (movieID, movieName)
      try {
        (fields(0).toInt, fields(1))
      } catch {
        case e: Exception => (0, " ")
      }
    }.collect.toMap
    movies
  }
}
