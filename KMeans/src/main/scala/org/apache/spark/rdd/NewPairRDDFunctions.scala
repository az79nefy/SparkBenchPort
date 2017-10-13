package org.apache.spark.rdd

import java.nio.ByteBuffer
import scala.reflect.ClassTag
import org.apache.spark._
import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.serializer.Serializer
import org.apache.spark.annotation.Experimental

import org.apache.spark.util.collection.CompactBuffer

/**
 * Extra functions available on ProvRDDs of (key, value) pairs through an implicit conversion.
 *
 * These are mostly wrapper functions of the original PairRDDFunctions returning ProvRDDs instead.
 */
class NewPairRDDFunctions[K, V](self: ProvRDD[(K, V)])(implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
    extends PairRDDFunctions(self) {

  def combineByKeyWithClassTagAnnotate[C](annotations: Seq[CombineAnnotation[V]])(
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C,
    partitioner: Partitioner,
    mapSideCombine: Boolean,
    serializer: Serializer)(implicit ct: ClassTag[C]): ProvRDD[(K, C)] = self.withScope {

    require(mergeCombiners != null, "mergeCombiners must be defined") // required as of Spark 0.9.0
    if (keyClass.isArray) {
      if (mapSideCombine) {
        throw new SparkException("Cannot use map-side combining with array keys.")
      }
      if (partitioner.isInstanceOf[HashPartitioner]) {
        throw new SparkException("Default partitioner cannot partition array keys.")
      }
    }
    val aggregator = new Aggregator[K, V, C](
      self.context.clean(createCombiner),
      self.context.clean(mergeValue),
      self.context.clean(mergeCombiners))
    if (self.partitioner == Some(partitioner)) {
      // TODO prov info here too
      ???
      //      self.mapPartitions(iter => {
      //        val context = TaskContext.get()
      //        new InterruptibleIterator(context, aggregator.combineValuesByKey(iter, context))
      //      }, preservesPartitioning = true)
    } else {
      val cpRDD = new CombineInfoRDD[K, V, C](self, partitioner, serializer, aggregator, mapSideCombine,
        createCombiner, mergeValue, mergeCombiners, annotations)
      new ProvRDD(cpRDD)
    }
  }

  override def combineByKeyWithClassTag[C](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C,
    partitioner: Partitioner,
    mapSideCombine: Boolean = true,
    serializer: Serializer = null)(implicit ct: ClassTag[C]): ProvRDD[(K, C)] =
    combineByKeyWithClassTagAnnotate(Seq())(createCombiner, mergeValue, mergeCombiners,
      partitioner, mapSideCombine, serializer)

  override def combineByKey[C](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C,
    partitioner: Partitioner,
    mapSideCombine: Boolean = true,
    serializer: Serializer = null): ProvRDD[(K, C)] =
    super.combineByKey(createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine, serializer).asInstanceOf[ProvRDD[(K, C)]]

  override def combineByKey[C](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C,
    numPartitions: Int): ProvRDD[(K, C)] =
    super.combineByKey(createCombiner, mergeValue, mergeCombiners, numPartitions).asInstanceOf[ProvRDD[(K, C)]]

  override def combineByKeyWithClassTag[C](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C,
    numPartitions: Int)(implicit ct: ClassTag[C]): ProvRDD[(K, C)] =
    super.combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners, numPartitions).asInstanceOf[ProvRDD[(K, C)]]

  override def combineByKey[C](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C): ProvRDD[(K, C)] =
    super.combineByKey(createCombiner, mergeValue, mergeCombiners).asInstanceOf[ProvRDD[(K, C)]]

  def combineByKeyAnnotate[C](annotations: Seq[CombineAnnotation[V]])(
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C): ProvRDD[(K, C)] =
    combineByKeyWithClassTagAnnotate(annotations)(createCombiner, mergeValue, mergeCombiners, defaultPartitioner(self), true, null)(null)

  override def combineByKeyWithClassTag[C](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C)(implicit ct: ClassTag[C]): ProvRDD[(K, C)] =
    super.combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners).asInstanceOf[ProvRDD[(K, C)]]

  override def aggregateByKey[U: ClassTag](zeroValue: U, partitioner: Partitioner)(seqOp: (U, V) => U,
    combOp: (U, U) => U): ProvRDD[(K, U)] = super.aggregateByKey(zeroValue, partitioner)(seqOp, combOp).asInstanceOf[ProvRDD[(K, U)]]

  override def aggregateByKey[U: ClassTag](zeroValue: U, numPartitions: Int)(seqOp: (U, V) => U,
    combOp: (U, U) => U): ProvRDD[(K, U)] = super.aggregateByKey(zeroValue, numPartitions)(seqOp, combOp).asInstanceOf[ProvRDD[(K, U)]]

  def aggregateByKeyAnnotate[U: ClassTag](annotations: Seq[CombineAnnotation[V]])(zeroValue: U)(seqOp: (U, V) => U,
    combOp: (U, U) => U): ProvRDD[(K, U)] = self.withScope {
    // Serialize the zero value to a byte array so that we can get a new clone of it on each key
    val zeroBuffer = SparkEnv.get.serializer.newInstance().serialize(zeroValue)
    val zeroArray = new Array[Byte](zeroBuffer.limit)
    zeroBuffer.get(zeroArray)

    lazy val cachedSerializer = SparkEnv.get.serializer.newInstance()
    val createZero = () => cachedSerializer.deserialize[U](ByteBuffer.wrap(zeroArray))

    // We will clean the combiner closure later in `combineByKey`
    val cleanedSeqOp = self.context.clean(seqOp)
    combineByKeyWithClassTagAnnotate[U](annotations :+ AggregateByKey[V, U](zeroValue))((v: V) => cleanedSeqOp(createZero(), v),
      cleanedSeqOp, combOp, defaultPartitioner(self), true, null)
  }

  override def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,
    combOp: (U, U) => U): ProvRDD[(K, U)] = aggregateByKeyAnnotate(Seq())(zeroValue)(seqOp, combOp)

  override def foldByKey(
    zeroValue: V,
    partitioner: Partitioner)(func: (V, V) => V): ProvRDD[(K, V)] =
    super.foldByKey(zeroValue, partitioner)(func).asInstanceOf[ProvRDD[(K, V)]]

  override def foldByKey(zeroValue: V, numPartitions: Int)(func: (V, V) => V): ProvRDD[(K, V)] =
    super.foldByKey(zeroValue, numPartitions)(func).asInstanceOf[ProvRDD[(K, V)]]

  override def foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)] =
    super.foldByKey(zeroValue)(func).asInstanceOf[ProvRDD[(K, V)]]

  def reduceByKeyAnnotate(annotations: Seq[CombineAnnotation[V]])(func: (V, V) => V): ProvRDD[(K, V)] =
    combineByKeyWithClassTagAnnotate[V](annotations :+ ReduceByKey())((v: V) => v, func, func, defaultPartitioner(self), true, null)

  override def reduceByKey(func: (V, V) => V): ProvRDD[(K, V)] = reduceByKeyAnnotate(Seq())(func)

  override def reduceByKey(func: (V, V) => V, numPartitions: Int): ProvRDD[(K, V)] =
    super.reduceByKey(func, numPartitions).asInstanceOf[ProvRDD[(K, V)]]

  override def reduceByKey(partitioner: Partitioner, func: (V, V) => V): ProvRDD[(K, V)] =
    super.reduceByKey(partitioner, func).asInstanceOf[ProvRDD[(K, V)]]

  override def groupByKey(partitioner: Partitioner): ProvRDD[(K, Iterable[V])] =
    super.groupByKey(partitioner).asInstanceOf[ProvRDD[(K, Iterable[V])]]

  override def groupByKey(numPartitions: Int): ProvRDD[(K, Iterable[V])] =
    super.groupByKey(numPartitions).asInstanceOf[ProvRDD[(K, Iterable[V])]]

  override def groupByKey(): ProvRDD[(K, Iterable[V])] = self.withScope {
    // groupByKey shouldn't use map side combine because map side combine does not
    // reduce the amount of data shuffled and requires all map side data be inserted
    // into a hash table, leading to more objects in the old gen.
    val createCombiner = (v: V) => CompactBuffer(v)
    val mergeValue = (buf: CompactBuffer[V], v: V) => buf += v
    val mergeCombiners = (c1: CompactBuffer[V], c2: CompactBuffer[V]) => c1 ++= c2
    val bufs = combineByKeyWithClassTagAnnotate[CompactBuffer[V]](Seq(GroupByKey()))(createCombiner, mergeValue, mergeCombiners, defaultPartitioner(self), false, null)
    bufs.asInstanceOf[ProvRDD[(K, Iterable[V])]]
  }

  override def countApproxDistinctByKey(p: Int, sp: Int, partitioner: Partitioner): ProvRDD[(K, Long)] =
    super.countApproxDistinctByKey(p, sp, partitioner).asInstanceOf[ProvRDD[(K, Long)]]

  override def countApproxDistinctByKey(relativeSD: Double, partitioner: Partitioner): ProvRDD[(K, Long)] =
    super.countApproxDistinctByKey(relativeSD, partitioner).asInstanceOf[ProvRDD[(K, Long)]]

  override def countApproxDistinctByKey(relativeSD: Double, numPartitions: Int): ProvRDD[(K, Long)] =
    super.countApproxDistinctByKey(relativeSD, numPartitions).asInstanceOf[ProvRDD[(K, Long)]]

  override def countApproxDistinctByKey(relativeSD: Double = 0.05): ProvRDD[(K, Long)] =
    super.countApproxDistinctByKey(relativeSD).asInstanceOf[ProvRDD[(K, Long)]]

  override def keys: ProvRDD[K] = super.keys.asInstanceOf[ProvRDD[K]]

  override def values: ProvRDD[V] = super.values.asInstanceOf[ProvRDD[V]]

  override def mapValues[U](f: V => U): ProvRDD[(K, U)] = self.withScope {
    val cleanF = self.context.clean(f)
    val kvF: ((K, V)) => (K, U) = { case (k, v) => (k, f(v)) }
    val rdd = new MapInfoRDD[(K, U), (K, V)](self,
      (context, pid, iter) => iter.map { case (k, v) => (k, cleanF(v)) }, kvF,
      preservesPartitioning = true)
    new ProvRDD(rdd)
  }

  /**
   * MapValues version accepting an inverse function to provide beter performance when computing the provenance.
   */
  def mapValuesWithInverse[U](f: V => U)(inverseF: U => V): ProvRDD[(K, U)] = self.withScope {
    val cleanF = self.context.clean(f)
    val inversekvF: ((K, U)) => (K, V) = { case (k, u) => (k, inverseF(u)) }
    val rdd = new InvertableMapInfoRDD[(K, U), (K, V)](self,
      (context, pid, iter) => iter.map { case (k, v) => (k, cleanF(v)) }, inversekvF,
      preservesPartitioning = true)
    new ProvRDD(rdd)
  }

  override def flatMapValues[U](f: V => TraversableOnce[U]): ProvRDD[(K, U)] = self.withScope {
    val cleanF = self.context.clean(f)
    val kvF: ((K, V)) => TraversableOnce[(K, U)] = { case (k, v) => for (t <- f(v)) yield (k, t) }
    val rdd = new FlatMapInfoRDD[(K, U), (K, V)](self,
      (context, pid, iter) => iter.flatMap {
        case (k, v) =>
          cleanF(v).map(x => (k, x))
      },
      kvF,
      preservesPartitioning = true)
    new ProvRDD(rdd)

  }

  /**
   * Return a copy of the RDD partitioned using the specified partitioner.
   *
   * This returns a provenance RDD whose provenance function will trace the data
   * of the previous RDD. Example:
   * val rdd : ProvRDD[(K,V)]
   * val prdd = rdd.partitionBy(...)
   *
   * Both rdd.provenance() and prdd.provenance() will return the same thing.
   *
   */
  override def partitionBy(partitioner: Partitioner): ProvRDD[(K, V)] = self.withScope {
    val superRDD = super.partitionBy(partitioner)
    // We keep on tracking the same data, while performing the repartition
    new ProvRDD(superRDD, self.trackedRDD)
  }

  def subtractByKey[W: ClassTag](other: ProvRDD[(K, W)], p: Partitioner): ProvRDD[(K, V)] = self.withScope {
    val rdd = new SubtractByKeyInfoRDD[K, V, W](self, other, p)
    new ProvRDD(rdd)
  }

  def subtractByKey[W: ClassTag](other: ProvRDD[(K, W)]): ProvRDD[(K, V)] = self.withScope {
    subtractByKey(other, self.partitioner.getOrElse(new HashPartitioner(self.partitions.length)))
  }

  def subtractByKey[W: ClassTag](
    other: ProvRDD[(K, W)],
    numPartitions: Int): ProvRDD[(K, V)] = self.withScope {
    subtractByKey(other, new HashPartitioner(numPartitions))
  }

  def cogroup[W: ClassTag](other: ProvRDD[(K, W)], partitioner: Partitioner): ProvRDD[(K, (Iterable[V], Iterable[W]))] = self.withScope {
    if (partitioner.isInstanceOf[HashPartitioner] && keyClass.isArray) {
      throw new SparkException("Default partitioner cannot partition array keys.")
    }
    val rdd = new Cogroup2InfoRDD(self, other, partitioner)
    new ProvRDD(rdd)
  }

  def cogroup[W1: ClassTag, W2: ClassTag](other1: ProvRDD[(K, W1)], other2: ProvRDD[(K, W2)], partitioner: Partitioner): ProvRDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = self.withScope {
    if (partitioner.isInstanceOf[HashPartitioner] && keyClass.isArray) {
      throw new SparkException("Default partitioner cannot partition array keys.")
    }
    val rdd = new Cogroup3InfoRDD(self, other1, other2, partitioner)
    new ProvRDD(rdd)
  }

  def cogroup[W1: ClassTag, W2: ClassTag, W3: ClassTag](other1: ProvRDD[(K, W1)],
    other2: ProvRDD[(K, W2)],
    other3: ProvRDD[(K, W3)],
    partitioner: Partitioner): ProvRDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] = self.withScope {
    if (partitioner.isInstanceOf[HashPartitioner] && keyClass.isArray) {
      throw new SparkException("Default partitioner cannot partition array keys.")
    }
    val rdd = new Cogroup4InfoRDD(self, other1, other2, other3, partitioner)
    new ProvRDD(rdd)
  }

  def cogroup[W: ClassTag](other: ProvRDD[(K, W)]): ProvRDD[(K, (Iterable[V], Iterable[W]))] = self.withScope {
    cogroup(other, defaultPartitioner(self, other))
  }

  def cogroup[W1: ClassTag, W2: ClassTag](other1: ProvRDD[(K, W1)], other2: ProvRDD[(K, W2)]): ProvRDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = self.withScope {
    cogroup(other1, other2, defaultPartitioner(self, other1, other2))
  }

  def cogroup[W1: ClassTag, W2: ClassTag, W3: ClassTag](other1: ProvRDD[(K, W1)], other2: ProvRDD[(K, W2)], other3: ProvRDD[(K, W3)]): ProvRDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] = self.withScope {
    cogroup(other1, other2, other3, defaultPartitioner(self, other1, other2, other3))
  }

  def cogroup[W: ClassTag](
    other: ProvRDD[(K, W)],
    numPartitions: Int): ProvRDD[(K, (Iterable[V], Iterable[W]))] = self.withScope {
    cogroup(other, new HashPartitioner(numPartitions))
  }

  def cogroup[W1: ClassTag, W2: ClassTag](other1: ProvRDD[(K, W1)], other2: ProvRDD[(K, W2)], numPartitions: Int): ProvRDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = self.withScope {
    cogroup(other1, other2, new HashPartitioner(numPartitions))
  }

  def cogroup[W1: ClassTag, W2: ClassTag, W3: ClassTag](other1: ProvRDD[(K, W1)],
    other2: ProvRDD[(K, W2)],
    other3: ProvRDD[(K, W3)],
    numPartitions: Int): ProvRDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] = self.withScope {
    cogroup(other1, other2, other3, new HashPartitioner(numPartitions))
  }

  def groupWith[W: ClassTag](other: ProvRDD[(K, W)]): ProvRDD[(K, (Iterable[V], Iterable[W]))] = self.withScope {
    cogroup(other, defaultPartitioner(self, other))
  }

  def groupWith[W1: ClassTag, W2: ClassTag](other1: ProvRDD[(K, W1)], other2: ProvRDD[(K, W2)]): ProvRDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = self.withScope {
    cogroup(other1, other2, defaultPartitioner(self, other1, other2))
  }

  def groupWith[W1: ClassTag, W2: ClassTag, W3: ClassTag](other1: ProvRDD[(K, W1)],
    other2: ProvRDD[(K, W2)], other3: ProvRDD[(K, W3)]): ProvRDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] = self.withScope {
    cogroup(other1, other2, other3, defaultPartitioner(self, other1, other2, other3))
  }

  def join[W: ClassTag](other: ProvRDD[(K, W)], partitioner: Partitioner): ProvRDD[(K, (V, W))] = self.withScope {
    val rdd = new JoinInfoRDD[K, V, W](self, other, partitioner)
    new ProvRDD(rdd)
  }

  def join[W: ClassTag](other: ProvRDD[(K, W)]): ProvRDD[(K, (V, W))] = self.withScope {
    join(other, defaultPartitioner(self, other))
  }

  def join[W: ClassTag](other: ProvRDD[(K, W)], numPartitions: Int): ProvRDD[(K, (V, W))] = self.withScope {
    join(other, new HashPartitioner(numPartitions))
  }

  /*
   * TODO missing functions:
   * 
   * sampleByKey
   * sampleByKeyExact
   * leftOuterJoin
   * rightOuterJoin
   * fullOuterJoin
   */

}