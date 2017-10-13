package org.apache.spark.sa

import java.io.InputStream
import org.apache.spark.rdd._
import scala.reflect.runtime.universe._
import scala.reflect.runtime.{ currentMirror => m }
import org.opalj.br._
import org.opalj.br.instructions._
import org.opalj.br.reader.Java8Framework.ClassFile
import scala.reflect.runtime.{ currentMirror => m }

/**
 * The Static Analyzer uses OPAL to analyze anonymous functions and infer Annotations of
 * combineByKey function.
 */
object StaticAnalyzer {

  /**
   * Infers missing annotations if possible by using Static Analysis.
   */
  def inferAnnotations[V, C](
    annotations: Seq[CombineAnnotation[V]],
    origCreateCombiner: V => C,
    origMergeValue: (C, V) => C,
    origMergeCombiners: (C, C) => C): Seq[CombineAnnotation[V]] = {

    annotations.foreach { println }
    if (annotations.exists { a => a.isInstanceOf[GroupByKey[V]] }) {
      // Nothing we can do. In groupByKey all input elements are needed.
      return Seq()
    }

    val f1 = getApplyMethod(getSourceFile(origCreateCombiner))
    val f2 = getApplyMethod(getSourceFile(origMergeValue))
    val f3 = getApplyMethod(getSourceFile(origMergeCombiners))

    if (f1.isEmpty || f2.isEmpty || f3.isEmpty) {
      return Seq()
    }

    // Test for Selection
    if (annotations.exists { a => a.isInstanceOf[ReduceByKey[V]] }
      && IsSelectAnalysis(f3.get)) {
      // XXX technically, a selection can also be achieved with combineByKey
      // But no sane person would program it that way
      return Selection[V] +: annotations
    }
    
    
    // Test for Aggregation (no guard)
    if (annotations.exists { a => a.isInstanceOf[ReduceByKey[V]] }) {
      val optionalZeroVal = IsAggregateAnalysis.isAggregatingCombiner[V](f3.get)
      if(optionalZeroVal.isDefined) {
        return Aggregation[V](optionalZeroVal.get) +: annotations
      }
    }

    Seq()
  }

  /**
   * Finds the source file in which the given anonymous function is defined. If the file cannot
   * be found, null is returned.
   */
  def getSourceFile(f: Any): InputStream = {
    val im = m reflect f
    val className = im.symbol.fullName
    val classFile = className.replace(".$$", "$$").replace(".$", "$$").replace('.', '/') + ".class"
    getClass().getClassLoader().getResourceAsStream(classFile)
  }

  /**
   * Gets the apply method of interest from the source file.
   */
  def getApplyMethod(is: InputStream): Option[(ClassFile, Method)] = {
    val cf = ClassFile(() => is)(0)
    val ms = cf.methods
    val m = ms.find { m => m.name.startsWith("apply$") }
      .orElse(ms.find { m => m.name == "apply" && !m.isSynthetic })
    m.map(method => (cf, method))
  }
}