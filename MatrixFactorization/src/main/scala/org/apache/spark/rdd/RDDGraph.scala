package org.apache.spark.rdd

import scala.collection.mutable.{ Map => MMap, Buffer }

/**
 * Represents graph information for a computation on ProvRDDs.
 */
private[spark] class RDDGraph(val start: ProvRDD[_]) {

  // This will contain the ordered seq afterwards
  private var topsortedRDDs: Seq[ProvRDD[_]] = Seq()
  // This will contain the ordered, filtered seq
  private var filteredRDDs: Seq[ProvRDD[_]] = Seq()
  // We collect also how each parent's provenance can be computed. This is, its dependences (children).
  private var childrenMap: MMap[ProvRDD[_], Buffer[(ProvRDD[_], Int)]] = MMap()
  // And the parents
  private var parentsMap: MMap[ProvRDD[_], Buffer[(ProvRDD[_], Int)]] = MMap()
  //
  private var startRDDCount: Long = 0

  // Invoke topsort to create the Graph
  topsort()

  // Case objects for the topsort algorithm
  private sealed trait TopSortMark
  private case object Temp extends TopSortMark
  private case object Perm extends TopSortMark

  /**
   * Computes the topological sort of the given RDD and its parents. It uses a DFS algorithm
   * described by Cormen et al. (2001);  Tarjan (1976).
   *
   * While doing that, it also collects the dependences of each RDD. That is, how the provenance
   * of an RDD can be computed from its children. It also collects the parents.
   */
  private def topsort() = {
    // During the traversal rdds can be temporarily and permanenly marked
    var marks: MMap[ProvRDD[_], TopSortMark] = MMap()

    visit(start)

    def visit(rdd: ProvRDD[_]): Unit = {
      // Get the mark of the currently visited RDD
      val mark = marks.get(rdd)
      // Obtain the parents
      val parents = rdd.trackedRDD match {
        case t: HasProvenance => t.provenanceParents
        case _ => Seq()
      }

      if (mark.filter { _ == Temp }.isDefined) ??? // stop (not a DAG)
      // if rdd is not marked (i.e. has not been visited yet) then
      if (mark.isEmpty) {
        // mark rdd temporarily
        marks += ((rdd, Temp))

        for (i <- 0 until parents.length) {
          val parent = parents(i)
          // Add a dependences to the this rdd for all parents
          if (!childrenMap.contains(parent)) {
            childrenMap += ((parent, Buffer()))
          }
          childrenMap(parent) += ((rdd, i))

          // Also save the opposite direction (the parents)
          if (!parentsMap.contains(rdd)) {
            parentsMap += ((rdd, Buffer()))
          }
          parentsMap(rdd) += ((parent, i))

          // visit all parents
          visit(parent)
        }
        // mark rdd permanently
        marks += ((rdd, Perm))
        // add rdd to head of list
        topsortedRDDs = rdd +: topsortedRDDs
      }
    }
  }

  /**
   * Get the children of an RDD. The integer in the tuple
   * denotes the dependencyNum of the child to the parent.
   *
   * Children are equivalent to provenance dependencies.
   */
  def getChildrenOf(rdd: ProvRDD[_]): Seq[(ProvRDD[_], Int)] = {
    childrenMap.get(rdd).map(_.toSeq).getOrElse(Seq())
  }

  /**
   * Get the parents of an RDD. The integer in the tuple
   * denotes the dependencyNum of the child to the parent.
   */
  def getParentsOf(rdd: ProvRDD[_]): Seq[(ProvRDD[_], Int)] = {
    parentsMap.get(rdd).map(_.toSeq).getOrElse(Seq())
  }

  /**
   * Given the graph, this function filters out any RDDs that must not be computed
   * to get the provenance for a set of named RDDs.
   */
  def filterRDDsWithDeps(rddNamesToCompute: Seq[String]) = {
    var remainingRDDNames = rddNamesToCompute.toBuffer

    // Traverse the list from the back and keep any rdd that is one to compute or in its dependency chain
    for (i <- topsortedRDDs.size - 1 to 0 by -1) {
      val elem = topsortedRDDs(i)
      // Tests if one of the (remaining) RDD names include the current elem
      if (remainingRDDNames.contains(elem.getProvenanceName)) {
        // Remove the name from the RDD names
        remainingRDDNames -= elem.getProvenanceName

        // Add elem to the result (at the front)
        filteredRDDs = elem +: filteredRDDs
      } // Tests if the dependences of the already included RDDs contain the current elem
      else if (filteredRDDs.map(getChildrenOf).exists(seq => seq.map(_._1).contains(elem))) {
        // Add elem to the result (at the front)
        filteredRDDs = elem +: filteredRDDs
      }
    }
    // Check if rdd names remain to be computed. In that case, throw an exception
    remainingRDDNames.headOption.foreach { name =>
      throw new IllegalArgumentException(s"$name does not denote a named RDD in the provenance")
    }
  }

  // ------------------------------------------
  // backwardsTrace function and helpers.
  // ------------------------------------------

  /**
   * Uses func to compute the provenance of all children. extractProv is used to get
   * the children computed provenance from the IterationInformation. Will be either its
   * origProvenance or recProvenance. Uses union to merge the provenances of the children.
   */
  def computeProvAndUnion(
    rdd: ProvRDD[_],
    children: Seq[(ProvRDD[_], Int)],
    func: (ProvRDD[_], Int) => ProvRDD[Any],
    extractProv: IterationInformation => ProvRDD[_]): ProvRDD[_] = {
    // Compute the provenance
    val rddsToMerge = children.map { case (dep, num) => func(extractProv(IterationInformation(dep)), num) }

    // FIXME we want to use coalesce here
    if (rddsToMerge.size == 1) rddsToMerge(0) else {
      // Union the dataItems in all rddsToMerge, but remove the duplicates by using subtract
      val dataRDD = rddsToMerge.map(_.dataRDD).reduce((rdd1, rdd2) =>
        rdd1.union(rdd2.subtract(rdd1))) //.coalesce(rdd1.getNumPartitions)

      new ProvRDD(dataRDD, rdd.trackedRDD.asInstanceOf[RDD[Any]])
    }
  }

  /**
   * Computes the backwards trace for a set of named RDDs, a provenance function, an initial filter and
   * optionally iterating to guarantee reproducibility.
   */
  def backwardsTrace(
    names: Seq[String],
    func: (ProvRDD[_], Int) => ProvRDD[Any],
    filter: Any => Boolean = (_ => true),
    iterate: Boolean = false): Map[String, ProvRDD[_]] = {

    // Make sure to remove the Byte-Order-Mark from text, otherwise
    // Spark will not behave deterministically,
    // Which can end in an endless loop

    val startRDD: ProvRDD[_] = new ProvRDD(start.asInstanceOf[ProvRDD[Any]].dataRDD.filter(filter),
      start.asInstanceOf[ProvRDD[Any]].trackedRDD)
    if (ProvRDD.cacheIterations) {
      startRDD.dataRDD.persist(ProvRDD.cacheLevel)
    }
    // Count, if iterate is enabled
    if (iterate) {
      startRDDCount = startRDD.dataRDD.count()
    }

    // Only the parent RDDs that need to be computed
    filterRDDsWithDeps(names)

    // Initially, we store information on the startRDD
    IterationInformation(start).origProvenance = startRDD

    // For all but the first (which is startRDD that has no dependences)
    for (rdd <- filteredRDDs.tail) {
      val children = getChildrenOf(rdd)
      val prov = computeProvAndUnion(rdd, children, func, _.origProvenance)
      if (iterate && ProvRDD.cacheIterations) {
        prov.dataRDD.persist(ProvRDD.cacheLevel)
      }
      // Store provenance result
      IterationInformation(rdd).origProvenance = prov
    }

    // XXX By uncommenting the lines with i (and commenting resultsDiverge),
    // you can test the overhead of checking if the results diverge each round,
    // compared to running a fixed number of rounds.
    //    var i = 0
    var rec: ProvRDD[_] = null
    while (iterate && {
      // Recompute with just the tracked input
      rec = recompute(start, names)
      // Check, if we get the expected result 
      resultsDiverge(rec, startRDD)
      //      i += 1
      //      i < n

    }) {
      println(s"Starting one iteration")
      // Compute a new BW trace.
      backwardsTraceOnCopy(rec, func, names)
    }

    // return map
    IterationInformation.buildMapOfNames(names)
  }

  /**
   * Recomputes a (sub)graph, using only the computed Provenance as input data instead of
   * the original input data, but uses the same transformations.
   */
  private def recompute(
    origOutRDD: ProvRDD[_],
    names: Seq[String]): ProvRDD[_] = {
    // For all the rdds that provenance was computed for,
    // But in reverse order (that is, starting with the parents)
    for (rdd <- filteredRDDs.reverse) {
      val recomputed: ProvRDD[_] = if (names.contains(rdd.getProvenanceName)) {
        // We have reached one RDD that provenance was requested for.
        // We return this provenance as the recomputation.
        IterationInformation(rdd.getProvenanceName).origProvenance
      } else if (getParentsOf(rdd).isEmpty) {
        // In the case where we encounter an RDD with no parents,
        // we take the original data unchanged
        rdd
      } else {
        // The RDD should implement HasProvenance
        rdd.trackedRDD match {
          case hp: HasProvenance => {
            val parents = hp.provenanceParents
            // All parents should have recomputed already
            val recomputedParents = parents.map { p => IterationInformation(p).getRecomputed }
            // build a new copy of trackedRDD with the new parents
            val copyTracked = hp.copyWithParents(recomputedParents)
            // ProvRDD that has this as dataRDD and trackedRDD
            val rec = new ProvRDD(copyTracked, false)
            if (ProvRDD.cacheIterations) {
              copyTracked.persist(ProvRDD.cacheLevel)
            }

            // The new intermediate records of start are not relevant
            if (rdd != start) {
              // Look for new intermediate records, that might be the reason the computation diverges (if it does)
              findNewIntermediateRecords(rdd, rec)
            }

            // the result is rec
            rec
          }
          case default => {
            throw new UnsupportedOperationException("A " + default.getClass.getSimpleName +
              " does not provide provenance information")
          }
        }
      }

      // Save recomputation
      IterationInformation(rdd).recomputation = recomputed
    }
    // Return for convenience
    IterationInformation(origOutRDD).getRecomputed
  }

  /**
   * Looks for any new intermediate records (subtraction recomputed - orig),
   * and saves them.
   */
  private def findNewIntermediateRecords(orig: ProvRDD[_], recomputed: ProvRDD[_]) = {
    // Records that appear in the recomputation, but not in the original
    val diff = recomputed.dataRDD.asInstanceOf[RDD[Any]].subtract(orig.trackedRDD.asInstanceOf[RDD[Any]])
    // Saving these, even though it might me an empty RDD is still faster than
    // calling .count() (which is an additional action), before ssaving them.

    // Save these records
    IterationInformation(orig).newIntermediates = diff
  }

  /**
   * Checks if the results from an original computation and a re-computation diverge.
   */
  private def resultsDiverge(recomputation: ProvRDD[_], original: ProvRDD[_]): Boolean = {
    // Intersection of tracked output and recomputed output
    val intersection: RDD[_] = ProvRDD.intersectWithDups(original.sparkContext,
      original.dataRDD.asInstanceOf[RDD[Any]], recomputation.dataRDD.asInstanceOf[RDD[Any]])
    // intersection size and startRDD size should be the same, otherwise results diverge
    startRDDCount != intersection.count()
  }

  /**
   * Computes the backwards trace on a recomputed copy of the graph.
   * While doing that, any intermediate records that appear in the copy, but not in the original,
   * and intersect the trace computed here, are used to update the original backwards trace.
   *
   * TODO maybe we can think about not including ALL relevant new intermediate equivalent records.
   * In many cases (even more when there's duplicates), we don't need all of them.
   * See uniqueChars2.txt
   */
  private def backwardsTraceOnCopy(recomputed: ProvRDD[_],
    func: (ProvRDD[_], Int) => ProvRDD[Any],
    names: Seq[String]) = {
    // Remove all changed markers from the last iteration
    // Set additional provenance from last iteration to null
    IterationInformation.information.foreach {
      case (_, info) =>
        info.changed = false
        info.additionalProvenance = null
    }

    // Initially, we store information on the startRDD
    IterationInformation(start).recProvenance = recomputed

    // For all but the first (which is startRDD that has no dependences)
    for (rdd <- filteredRDDs.tail) {
      val children = getChildrenOf(rdd)
      val recProv = computeProvAndUnion(rdd, children, func, _.recProvenance)
      if (ProvRDD.cacheIterations) {
        recProv.dataRDD.persist(ProvRDD.cacheLevel)
      }

      val info = IterationInformation(rdd)
      info.recProvenance = recProv

      // If any of the children's ORIGINAL provenance changed, we have to recalculate the
      // parents original provenance, too.
      if (children.exists { case (prdd, _) => IterationInformation(prdd).changed }) {

        if (ProvRDD.iterationComputeProvenanceIndividually) {
          // Compute the provenance from the new items of the children, only
          info.additionalProvenance = computeProvAndUnion(rdd, children, func, _.additionalProvenance)
        } else {
          // Recompute with all items from the children
          val prov = computeProvAndUnion(rdd, children, func, _.origProvenance)
          // When merging the provenance that resulted from changed children's provenance
          // with the current one (that may have changed due to new intermediate records),
          // we need to unify them, but without adding too many duplicates. Therefore,
          // we subtract (preserving dups) first and then union.
          val subtracted = ProvRDD.subtractWithDups(prov.sparkContext, prov.dataRDD.asInstanceOf[RDD[Any]], info.origProvenance.dataRDD.asInstanceOf[RDD[Any]])
            .coalesce(info.origProvenance.dataRDD.getNumPartitions / 2)

          // We use coalesce to avoid empty partitions here, too.
          val newProvData = info.origProvenance.dataRDD.asInstanceOf[RDD[Any]].coalesce(info.origProvenance.dataRDD.getNumPartitions / 2)
            .union(subtracted)
          // XXX I want to use coalesce HERE, but get a strange exception
          //.coalesce(info.origProvenance.dataRDD.getNumPartitions)

          info.origProvenance = new ProvRDD(newProvData, prov.trackedRDD.asInstanceOf[RDD[Any]])
        }

        // mark this as changed
        info.changed = true

      }

      // If there are new intermediate records, we intersect them with the backwards trace on the copy,
      // to see if they could have influenced a diverging result
      if (info.newIntermediates != null) {
        info.relevantNewIntermediates = info.newIntermediates.asInstanceOf[RDD[Any]]
          .intersection(recProv.dataRDD.asInstanceOf[RDD[Any]])

        // If the rdd is a the result of combineByKey or cogroup (non-monotonic functions),
        // it could have introduced the new intermediate records. If not, the cause must
        // have been earlier in the graph. In the case of combine or cogroup, we find the
        // records in the original computation that are equivalent to the new intermediate
        // ones (they have the same key!), and include them in the original BW trace.
        info.origRDD.trackedRDD match {
          case rdd: NonMonotonic[Any, Any] => {
            // Compute equivalent records
            val equiv = rdd.equivalentRecords(info.relevantNewIntermediates.asInstanceOf[RDD[(Any, Any)]])

            if (ProvRDD.iterationComputeProvenanceIndividually) {
              val additionalProvenance =
                if (info.additionalProvenance != null) {
                  // Merge with the additional provenance from tracing back other new intermediate results
                  info.additionalProvenance.dataRDD.asInstanceOf[RDD[(Any, Any)]]
                    .union(equiv)
                    // We use coalesce to avoid many empty partitions.
                    // This saves enourmous amounts of time
                    .coalesce(info.origProvenance.dataRDD.getNumPartitions)
                    // Not retaining any duplicates
                    // This is important (otherwise me wight track more than can be produced actually)
                    // And it is possible to use distinct, because the Non-Monotonic transformations guarantee that each
                    // key exists only once afterwards.
                    .distinct()
                } else {
                  equiv
                }
              info.additionalProvenance =
                new ProvRDD(additionalProvenance, info.origProvenance.trackedRDD.asInstanceOf[RDD[(Any, Any)]])
            } else {
              // Add to original BW trace
              val dataRDD = info.origProvenance.dataRDD.asInstanceOf[RDD[(Any, Any)]]
                .union(equiv)
                .coalesce(info.origProvenance.dataRDD.getNumPartitions)
                .distinct()
              info.origProvenance = new ProvRDD(dataRDD, info.origProvenance.trackedRDD.asInstanceOf[RDD[(Any, Any)]])
            }

            // Mark as changed
            info.changed = true

          }
          case default => {
            // Nothing to to here
          }
        }
      }

      if (ProvRDD.iterationComputeProvenanceIndividually && names.contains(rdd.getProvenanceName)) {
        // Here we need to override the original provenance by merging it with the additional provenance
        val subtracted = ProvRDD.subtractWithDups(info.additionalProvenance.sparkContext,
          info.additionalProvenance.dataRDD.asInstanceOf[RDD[Any]], info.origProvenance.dataRDD.asInstanceOf[RDD[Any]])
          .coalesce(info.origProvenance.dataRDD.getNumPartitions / 2)
        val newProvData = info.origProvenance.dataRDD.asInstanceOf[RDD[Any]]
          .coalesce(info.origProvenance.dataRDD.getNumPartitions / 2)
          .union(subtracted)
        info.origProvenance = new ProvRDD(newProvData, info.origProvenance.trackedRDD.asInstanceOf[RDD[Any]])
      }
    }

  }

  /**
   * Companion object to obtain iteration information by RDD or name.
   */
  private object IterationInformation {
    var information: MMap[ProvRDD[_], IterationInformation] = MMap()
    def apply(prdd: ProvRDD[_]) = information.getOrElseUpdate(prdd, new IterationInformation(prdd))
    def apply(name: String) = information.find(_._1.getProvenanceName == name).get._2
    def buildMapOfNames(names: Seq[String]): Map[String, ProvRDD[_]] = {
      var resultMap = MMap[String, ProvRDD[_]]()
      for ((prdd, info) <- information) {
        if (names.contains(prdd.getProvenanceName)) {
          resultMap += ((prdd.getProvenanceName, info.origProvenance))
        }
      }
      resultMap.toMap
    }
  }

  /**
   * Provides all information needed during provenance computation and optional iteration for one RDD in
   * the graph.
   *
   * The origRDD is the reference, the one that provenance is being computed for.
   */
  private class IterationInformation(val origRDD: ProvRDD[_]) {
    // The provenance of origRDD
    var origProvenance: ProvRDD[_] = null
    // Additional provenance items, to be added to origProvenance
    var additionalProvenance: ProvRDD[_] = null

    // Indicates whether origProvenance changed since the last round of iteration.
    var changed: Boolean = false
    // An optional recomputation with just the tracked elements
    var recomputation: ProvRDD[_] = null
    /**
     * If a parent does not need to be recomputed, (not part of the relevant subgraph),
     * The "recomputation" should return the unchanged parent instead
     */
    def getRecomputed = if (recomputation != null) recomputation else origRDD

    // Any intermediate records that appear in the recomputation, but not in the original
    var newIntermediates: RDD[_] = null
    // Only those intermediate records that intersect the new BW trace (recProvenance)
    var relevantNewIntermediates: RDD[_] = null
    // The provenance of recomputation
    var recProvenance: ProvRDD[_] = null
  }

}
