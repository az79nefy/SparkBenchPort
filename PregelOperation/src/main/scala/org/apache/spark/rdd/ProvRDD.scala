package org.apache.spark.rdd

import scala.reflect.{ ClassTag, classTag }
import org.apache.spark.rdd._
import org.apache.spark.Partition
import org.apache.spark.Partitioner
import org.apache.spark.HashPartitioner
import org.apache.spark.TaskContext
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

/**
 * Defines implicit functions that provide extra functionalities (and all the provenance query functions in general)
 * on ProvRDDs of specific types.
 */
object ProvRDD {

  // --------------
  // Configurations
  // --------------
  
  /** Cache level for all cacheing */
  var cacheLevel = StorageLevel.MEMORY_ONLY

  /** If the normal ProvRDD transformations should cache their results for faster provenance computation */
  var cacheComputation = false

  /** If the iterative process should cache intermediate results */
  var cacheIterations = false
  
  /** If SA should be used */
  var useStaticAnalyis = true
  
  /**
   * This setting can be used to measure the impact of this improvement. By default it should be turned on (true).
   * If false, in each iteration and for each RDD the origProvenance and the one from new intermediate records are
   * merged.
   * If true, the new intermediate records are traced back without merging. They are then only merged at the last
   * RDD of an iteration.
   */
  var iterationComputeProvenanceIndividually = true

  // Implicit functions

  implicit def rddToNewPairRDDFunctions[K, V](rdd: ProvRDD[(K, V)])(implicit kt: ClassTag[K], vt: ClassTag[V]): NewPairRDDFunctions[K, V] = {
    new NewPairRDDFunctions(rdd)
  }
  
  implicit def rddToNewOrderedRDDFunctions[K : Ordering : ClassTag, V: ClassTag](rdd: ProvRDD[(K, V)])
    : NewOrderedRDDFunctions[K, V, (K, V)] = {
    new NewOrderedRDDFunctions[K, V, (K, V)](rdd)
  }

  implicit def rddToRDDProvFunctions[T](rdd: ProvRDD[T])(implicit tt: ClassTag[T]): RDDProvFunctions[T] = {
    new RDDProvFunctions(rdd)
  }

  /**
   * An intersection preserving duplicates. Implemented on RDDs rather than ProvRDDs since it is used
   * to compute provenance only.
   */
  private[spark] def intersectWithDups[T: ClassTag](sc: SparkContext, rdd1: RDD[T], rdd2: RDD[T]): RDD[T] = sc.withScope {
    val cogrouped = rdd1.map(k => (k, null)).cogroup(rdd2.map(k => (k, null)))
    val groupSize = cogrouped.map { case (key, (buf1, buf2)) => (key, math.min(buf1.size, buf2.size)) }
    groupSize.flatMap { case (key, size) => List.fill(size)(key) }
  }

  /**
   * An subtraction preserving duplicates. Implemented on RDDs rather than ProvRDDs since it is used
   * to compute provenance only.
   *
   * rdd1 - rdd2
   */
  private[spark] def subtractWithDups[T: ClassTag](sc: SparkContext, rdd1: RDD[T], rdd2: RDD[T]): RDD[T] = sc.withScope {
    val rdd1Counts = rdd1.map((_, 1)).reduceByKey(_ + _)
    val rdd2Counts = rdd2.map((_, 1)).reduceByKey(_ + _)

    val joined = rdd1Counts.leftOuterJoin(rdd2Counts)
    joined.flatMap({ case (elem, (num, numMinus)) => for (i <- numMinus.getOrElse(0) until num) yield elem })
  }

  /** Build the union of a list of RDDs. Adapted from SparkContext.union */
  private[spark] def union[T: ClassTag](sc: SparkContext, rdds: Seq[ProvRDD[T]]): ProvRDD[T] = sc.withScope {
    val partitioners = rdds.flatMap(_.partitioner).toSet
    if (rdds.forall(_.partitioner.isDefined) && partitioners.size == 1) {
      // TODO prov info here too
      ???
      //new PartitionerAwareUnionRDD(this, rdds)
    } else {
      val rdd = new UnionInfoRDD(sc, rdds)
      new ProvRDD(rdd)
    }
  }

}

/**
 * A ProvRDD is a wrapper of the RDD dataRDD (decorator pattern). It can be used to provide provenance
 * information.
 *
 * The usual RDD functions like map are changed to return ProvRDDs themselves, that in turn also track
 * provenance information.
 *
 * The field dataRDD is the decorated RDD (that is, all calls are forwarded to it). The field
 * trackedRDD is a reference to the RDD in the original computation, that dataRDD is a subset of.
 * Both fields may be identical.
 */
class ProvRDD[T: ClassTag](private[spark] val dataRDD: RDD[T], private[spark] val trackedRDD: RDD[T])
    extends RDD[T](dataRDD.sparkContext, dataRDD.dependencies) {


  /**
   * Create a ProvRDD which dataRDD==trackedRDD, and specify if it should be cached.
   * 
   * This constructor is mainly used to create ProvRDDs manually, and it that case they should be cached
   * (depending on the policy).
   */
  def this(dataRDD: RDD[T], cacheMe: Boolean = true) = {
    this(dataRDD, dataRDD)
    if (cacheMe && ProvRDD.cacheComputation) {
      dataRDD.persist(ProvRDD.cacheLevel)
    }
  }

  // Implement abstract methods by forwarding to the backed RDD
  override def compute(split: Partition, context: TaskContext): Iterator[T] = dataRDD.compute(split, context)
  override def getPartitions: Array[Partition] = dataRDD.partitions

  // ---------------------------------------------------------------------------------
  // FORWARDING OF ACTIONS TODO other actions
  // ---------------------------------------------------------------------------------
  override def collect() = dataRDD.collect()

  /**
   *  Allow setting a name on the trackedRDD to identify it later
   */
  def setProvenanceName(name: String): this.type = {
    trackedRDD.name = name
    this
  }

  /**
   * Get the provenance name that was set on the trackedRDD.
   */
  def getProvenanceName: String = trackedRDD.name

  // ---------------------------------------------------------------------------------
  // WRAPPERS OF RDD FUNCTIONS
  // ---------------------------------------------------------------------------------

  override def map[U: ClassTag](f: T => U): ProvRDD[U] = withScope {
    val cleanF = sparkContext.clean(f)
    val rdd = new MapInfoRDD[U, T](this, (context, pid, iter) => iter.map(cleanF), f)
    new ProvRDD(rdd)
  }

  /**
   * Map version accepting an inverse function to provide beter performance when computing the provenance.
   */
  def mapWithInverse[U: ClassTag](f: T => U)(inverseF: U => T): ProvRDD[U] = withScope {
    val cleanF = sparkContext.clean(f)
    val rdd = new InvertableMapInfoRDD[U, T](this, (context, pid, iter) => iter.map(cleanF), inverseF)
    new ProvRDD(rdd)
  }

  // keyBy is a map function, that has a well-defined inverse function
  override def keyBy[K](f: T => K): ProvRDD[(K, T)] = mapWithInverse(a => (f(a), a))(_._2)

  override def flatMap[U: ClassTag](f: T => TraversableOnce[U]): ProvRDD[U] = withScope {
    val cleanF = sparkContext.clean(f)
    val rdd = new FlatMapInfoRDD[U, T](this, (context, pid, iter) => iter.flatMap(cleanF), f)
    new ProvRDD(rdd)
  }

  override def filter(f: T => Boolean): ProvRDD[T] = withScope {
    val cleanF = sparkContext.clean(f)
    val rdd = new FilterInfoRDD[T](this, (context, pid, iter) => iter.filter(cleanF), preservesPartitioning = true)
    new ProvRDD(rdd)
  }

  override def distinct(numPartitions: Int)(implicit ord: Ordering[T] = null): ProvRDD[T] = withScope {
    val rdd = new DistinctInfoRDD[T](this, numPartitions)
    new ProvRDD(rdd)
  }

  override def distinct(): ProvRDD[T] = super.distinct().asInstanceOf[ProvRDD[T]]

  def intersection(other: ProvRDD[T]): ProvRDD[T] = withScope {
    val rdd = new IntersectionInfoRDD[T](this, other, None)
    new ProvRDD(rdd)
  }

  def intersection(other: ProvRDD[T], partitioner: Partitioner)(implicit ord: Ordering[T]): ProvRDD[T] = withScope {
    val rdd = new IntersectionInfoRDD[T](this, other, Some(partitioner))
    new ProvRDD(rdd)
  }

  def intersection(other: ProvRDD[T], numPartitions: Int): ProvRDD[T] = withScope {
    intersection(other, new HashPartitioner(numPartitions))(null)
  }

  /** Build the union of this and a list of RDDs passed as variable-length arguments. */
  def union(first: ProvRDD[T], rest: ProvRDD[T]*): ProvRDD[T] = withScope {
    ProvRDD.union(sparkContext, Seq(this, first) ++ rest)
  }

  def ++(other: ProvRDD[T]): ProvRDD[T] = withScope {
    this.union(other)
  }

  /**
   * See RDD.coalesce
   *
   * This returns a provenance RDD whose provenance function will trace the data
   * of the previous RDD.
   * 
   * FIXME strange serialization bugs when using this
   */
  override def coalesce(numPartitions: Int, shuffle: Boolean = false,
    partitionCoalescer: Option[PartitionCoalescer] = Option.empty)(implicit ord: Ordering[T] = null): ProvRDD[T] = withScope {
    val superRDD = dataRDD.coalesce(numPartitions, shuffle, partitionCoalescer)(ord)
    // We keep on tracking the same data, while performing the coalesce
    new ProvRDD(superRDD, trackedRDD)
  }

  override def repartition(numPartitions: Int)(implicit ord: Ordering[T] = null): ProvRDD[T] =
    super.repartition(numPartitions)(ord).asInstanceOf[ProvRDD[T]]

  def cartesian[U: ClassTag](other: ProvRDD[U]): ProvRDD[(T, U)] = withScope {
    val rdd = new CartesianInfoRDD(sparkContext, this, other)
    new ProvRDD(rdd)
  }

  def subtract(other: ProvRDD[T], p: Partitioner)(implicit ord: Ordering[T]): ProvRDD[T] = withScope {
    if (partitioner == Some(p)) {
      val p2 = new Partitioner() {
        override def numPartitions: Int = p.numPartitions
        override def getPartition(k: Any): Int = p.getPartition(k.asInstanceOf[(Any, _)]._1)
      }
      val rdd = new SubtractInfoRDD[T](this, other, p2)
      new ProvRDD(rdd)
    } else {
      val rdd = new SubtractInfoRDD[T](this, other, p)
      new ProvRDD(rdd)
    }
  }

  def subtract(other: ProvRDD[T]): ProvRDD[T] = withScope {
    subtract(other, partitioner.getOrElse(new HashPartitioner(partitions.length)))(null)
  }

  def subtract(other: ProvRDD[T], numPartitions: Int): ProvRDD[T] = withScope {
    subtract(other, new HashPartitioner(numPartitions))(null)
  }

  /*
   * TODO missing transformations:
   * 
   * sample
   * randomSplit
   * randomSampleWithRange
   * sortBy
   * glom
   * groupBy
   * pipe
   * mapPartitions
   * mapPartitionsWithIndex
   * zip
   * zipPartitions
   * collect
   * zipWithIndex
   * zipWithUniqueId
   * 
   * In OrderedRDDFunctions:
   * filterByRange
   * repartitionAndSortWithinPartitions
   */

}