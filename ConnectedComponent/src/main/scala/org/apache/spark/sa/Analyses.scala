package org.apache.spark.sa

import org.opalj.br._
import org.opalj.br.analyses._
import org.opalj.br.instructions._
import org.opalj.ai._
import org.opalj.ai.domain._
import org.opalj.ai.domain.l1._
import org.opalj.ai.domain.l0._

import org.opalj.ai.Domain
import org.opalj.ai.InterruptableAI
import org.opalj.br.analyses.BasicReport
import org.opalj.br.analyses.DefaultOneStepAnalysis
import org.opalj.br.analyses.Project
import org.opalj.util.PerformanceEvaluation.time
import org.opalj.util.Seconds
import org.opalj.br.instructions.ReturnInstruction


/**
 * Shared functions by all analyses.
 */
object Analyses {

  /**
   * Calculates the local variable index where a parameter is stored.
   */
  private[sa] def parameterIndexToLocalVariableIndex(
    isStatic: Boolean,
    descriptor: MethodDescriptor,
    parameterIndex: Int): Int = {
    var localVariableIndex = 0

    if (!isStatic) {
      localVariableIndex += 1 /*=="this".computationalType.operandSize*/
    }
    val parameterTypes = descriptor.parameterTypes
    var currentIndex = 0
    while (currentIndex < parameterIndex) {
      localVariableIndex += parameterTypes(currentIndex).computationalType.operandSize
      currentIndex += 1
    }
    localVariableIndex
  }

  /**
   * Calculates the initial "ValueOrigin" associated with a method's parameter.
   * The index of the first parameter is 0 (which contains the implicit `this`
   * reference in case of instance methods).
   *
   * Copied from develop branch of org.opalj.ai.package.scala
   *
   * @param 	isStatic `true` if method is static and, hence, has no implicit
   *      	parameter for `this`.
   * @see 	[[mapOperandsToParameters]]
   */
  private[sa] def parameterIndexToValueOrigin(
    isStatic: Boolean,
    descriptor: MethodDescriptor,
    parameterIndex: Int): Int = {
    def origin(localVariableIndex: Int) = -localVariableIndex - 1

    val localVariableIndex = parameterIndexToLocalVariableIndex(isStatic, descriptor, parameterIndex)

    origin(localVariableIndex)
  }

  /**
   * Reverses the usedBy graph to get the definition sites for each use site, stored in a Map.
   */
  private[sa] def defSitesMap(fromIndex: Int, untilIndex: Int, domain: Domain with RecordDefUse): Map[Int, Set[Int]] = {
    var map = Map[Int, Set[Int]]()
    for (i <- fromIndex until untilIndex) {
      val usedBy = domain.usedBy(i)
      if (usedBy != null) {
        for (j <- usedBy) {
          // def @i / use @j
          if (map.isDefinedAt(j)) {
            val set = map(j) + i
            map = map + ((j, set))
          } else {
            val set = Set(i)
            map = map + ((j, set))
          }
        }
      }
    }
    map
  }

}