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

import com.sun.org.apache.bcel.internal.generic.LoadInstruction

/**
 * Analysis to determine if a function is an aggregate function
 */
object IsAggregateAnalysis {

  val noGuard: Any => Boolean = _ => true

  /**
   * Checks if a Combiner is an aggregate function. If so, it returns the zeroValue. Otherwise None.
   *
   * Detects sum and product of primitive types.
   *
   * We assume the method complies to the signature
   * (T, T) => T. If this is not the case, the behavior
   * is undefined. Also, the method needs to be defined as an anonymous function.
   */
  def isAggregatingCombiner[V](fun: (ClassFile, Method)): Option[V] = {
    val (classFile, method) = fun

    val domain = new IsAggregateDomain(classFile, method)

    val result = BaseAI(classFile, method, domain)
    val instructions = result.domain.code.instructions
    val isStatic = method.isStatic
    val descriptor = method.descriptor

    // Local variable indices
    val param0 = Analyses.parameterIndexToLocalVariableIndex(isStatic, descriptor, 0)
    val param1 = Analyses.parameterIndexToLocalVariableIndex(isStatic, descriptor, 1)

    if (method.parameterTypes.forall(_.isBaseType) && instructions.length == 4) {
      // For primitive types, there should be exactly 4 instructions, e.g.:
      //(DLOAD_1,0) loading param0
      //(DLOAD_3,1) loading param1
      //(DMUL,2) aggregating
      //(DRETURN,3) returning
      val testIns = instructions.zipWithIndex.forall {
        _ match {
          case (i: LoadLocalVariableInstruction, 0) => i.lvIndex == param0
          case (i: LoadLocalVariableInstruction, 1) => i.lvIndex == param1
          case (i: MultiplyInstruction, 2) => true
          case (i: AddInstruction, 2) => true
          case (i: ReturnInstruction, 3) => true
          case _ => false
        }
      }

      if (testIns) {
        if (instructions(2).isInstanceOf[MultiplyInstruction]) {
          // 1 is the zeroValue
          return Some(1.asInstanceOf[V])
        } else if (instructions(2).isInstanceOf[AddInstruction]) {
          // 0 is the zeroValue
          return Some(0.asInstanceOf[V])
        }
      }
    } 

    None
  }

  /**
   * Specially crafted domain with capabilities to determine if a function
   * is an aggregate function.
   */
  class IsAggregateDomain[Source](
    val classFile: ClassFile,
    val method: Method)
      extends CorrelationalDomain
      with org.opalj.ai.TheClassHierarchy
      with TheMethod
      with DefaultDomainValueBinding
      with ThrowAllPotentialExceptionsConfiguration
      with DefaultHandlingOfMethodResults
      with IgnoreSynchronization
      with l0.DefaultTypeLevelFloatValues
      with l0.DefaultTypeLevelDoubleValues
      with l0.TypeLevelFieldAccessInstructions
      with l0.TypeLevelInvokeInstructions
      with SpecialMethodsHandling
      with l1.ReferenceValues
      with l1.DefaultClassValuesBinding
      with l1.DefaultIntegerRangeValues
      with l1.DefaultLongValues
      with l1.LongValuesShiftOperators
      with l1.ConcretePrimitiveValuesConversions
      with GlobalLogContextProvider {
    def classHierarchy = org.opalj.br.ClassHierarchy(Seq(classFile))
  }
}