package inputdata

import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD

/**
 * Created by Ondra Fiedler on 27.8.14.
 */

/**
 * Loads and holds the MovieLens data (http://grouplens.org/datasets/movielens/)
 * @param dataDirectoryPath Path to the directory with MovieLens data. This directory contain file "ratings.dat" with ratings and file "movies.dat" with mapping between movie IDs and names
 */
class MovieLensDataHolder(dataDirectoryPath: String) extends DataHolder with Serializable {

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