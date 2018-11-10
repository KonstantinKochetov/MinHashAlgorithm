package algorithms

import scala.util.Random


// TODO: KLAUSUR RELEVANT
object JaccardSimilarity {

  val randgen = new Random

  /*
   * 
   * Calculate the Jaccard Distance of two Sets
   * 
   */
  def calculateJaccardDistanceSet[T](set1: Set[T], set2: Set[T]): Double = {
    // J(A,B) = |A∩B| / |AUB| = x / (x+y)

    set1.intersect(set2).size / set1.union(set2).size.toDouble

  }


  /*
   *
  * Calculate the Jaccard Distance of two Bags
  *
  */

  def calculateJaccardDistanceBag[T](as: Iterable[T], bs: Iterable[T]): Double = {

    // https://codereview.stackexchange.com/questions/75751/refactor-jaccard-similarity-the-scala-way
    val (xs, ys) = if (as.size <= bs.size) (as, bs) else (bs, as)

    val xCounts = xs.groupBy(x => x).map(x => x._1 -> x._2.size)

    val mins = ys.foldLeft(Map(): Map[T, Int]) { (ms, y) =>
      lazy val yCount: Int = ms.getOrElse(y, 0)
      if (xCounts.contains(y) && xCounts(y) > yCount) {
        ms.updated(y, yCount + 1)
      } else {
        ms
      }
    }

    mins.valuesIterator.sum.toDouble / (as.size + bs.size)


  }

  /*
   * 
   * Calculate an Array of Hash Functions
   * 
   * Each function of the array should have the following structure
   * h(x)= m*x + b mod c, where 
   *    
   *    m is random integer 
   *    b is a random integer
   *    c is the parameter nrHashFuns, that is passed in the signature of the method
   */
  // number of hash functions is "size"
  def createHashFuntions(size: Integer, nrHashFuns: Int): Array[Int => Int] = {

    val c = nrHashFuns

    val h = (x: Int) => {
      val m = Random.nextInt
      val b = Random.nextInt
      m * x + b % c
    }

    val result = List.fill(size)(h).toArray
    result
  }

  /*
   * Implement the MinHash algorithm presented in the lecture
   * 
   * Input:
   * matrix: Document vectors (each column should corresponds to one document)
   * hFuns: Array of Hash-Functions
   * 
   * Output:
   * Signature Matrix:
   * columns: Each column corresponds to one document
   * rows: Each row corresponds to one hash function
   */

  def minHash[T](matrix: Array[Array[Int]], hFuns: Array[Int => Int]): Array[Array[Int]] = {

    val numOfRows = hFuns.length
    val numOfColumns = matrix(0).length

    val signatureMatrix = Array.ofDim[Int](numOfRows, numOfColumns) // initialize result matrix with 0
    for ((row, i) <- signatureMatrix.zipWithIndex) {
      for ((column, j) <- row.zipWithIndex) {
          signatureMatrix(i)(j) = -1 // set all elements to -1
      }
    }

    for ((row, i) <- matrix.zipWithIndex) { // loop throw row -> hash -> columns. Applying hash to s1-s4 columns then go to next hash then agaon s1-s4 then do with the second row
      for ((h, k) <- hFuns.zipWithIndex) {
        for ((column, j) <- row.zipWithIndex) {
          val hash = h(i)

//          val temp0 = matrix(i)(j)
          if (matrix(i)(j) == 1) {
            if (hash <= matrix(i)(j) || signatureMatrix(k)(j) == -1) {
              signatureMatrix(k)(j) = hash
            }
          }
//          val temp1 = 0
        }
      }
    }

    signatureMatrix
  }

  /*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
   * 
   * Helper functions that are used in the tests
   * 
   * +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
   */

  def printMultipleSets(data: Array[Array[Int]]): Unit = {
    data.foreach(x => println(x.mkString(" ")))
  }

  def createRandomSetAsArray(nrElements: Int): Array[Int] = {
    val res = Array.tabulate(nrElements)(_ => 0)
    (for (i <- 0 to nrElements - 1) {

      if (randgen.nextFloat < 0.3) res(randgen.nextInt(nrElements - 1)) = 1
    })
    res
  }

  def transformArrayIntoSet(data: Array[Int]): Set[Int] = {

    (for (i <- 0 to data.size - 1 if (data(i) == 1)) yield i).toSet

  }

  def findNextPrim(x: Int): Int = {

    def isPrim(X: Int, i: Int, Max: Int): Boolean = {
      if (i >= Max) true
      else if (X % i == 0) false
      else isPrim(X, i + 1, Max)
    }

    if (isPrim(x, 2, math.sqrt(x).toInt + 1)) x
    else findNextPrim(x + 1)
  }

  def compareSignatures(sig1: Array[Int], sig2: Array[Int]): Int = {

    var res = 0
    for (i <- 0 to sig1.size - 1) {
      if (sig1(i) == sig2(i)) res = res + 1
    }
    res
  }

}