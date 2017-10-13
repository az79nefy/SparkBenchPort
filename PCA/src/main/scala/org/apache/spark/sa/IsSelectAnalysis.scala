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
 * Analysis to determine if a function is a select function
 */
object IsSelectAnalysis {

  /**
   * A method is a select method, if all return statements return one of the arguments,
   * unmodified.
   *
   * We assume the method complies to the signature
   * (T, T) => T. If this is not the case, the behavior
   * is undefined. Also, the method needs to be defined as an anonymous function.
   */
  def apply(fun: (ClassFile, Method)): Boolean = {
    val (classFile, method) = fun

    val domain = new IsSelectDomain(classFile, method)

    val result = BaseAI(classFile, method, domain)
    val instructions = result.domain.code.instructions
    val isStatic = method.isStatic
    val descriptor = method.descriptor

    // Origin values
    val param0 = Analyses.parameterIndexToValueOrigin(isStatic, descriptor, 0)
    val param1 = Analyses.parameterIndexToValueOrigin(isStatic, descriptor, 1)

    // Obtain the use->def map
    val defMap = Analyses.defSitesMap(param1, instructions.length, result.domain)

    // All instructions that may be returned by some return instruction
    val returnParams = instructions.zipWithIndex
      .filter { case (instruction, index) => instruction.isInstanceOf[ReturnInstruction] }
      .map {
        case (instruction, index) =>
          val definedBy = defMap.get(index)
          if (definedBy.isDefined) {
            definedBy.get
          } else {
            // This branch should never execute
            Set[Int]()
          }
      }
      .reduce(_ union _)

    // Check if all return instructions only return parameters
    if (returnParams.exists(defSite => defSite != param0 && defSite != param1)) {
      return false
    }

    // All parameters (that can be returned) that are mutable objects
    val mutableReturnParams = returnParams
      .map(insNum => if (insNum == param0) 0 else if (insNum == param1) 1 else -1)
      .filter(num => {
        val typ = method.parameterTypes(num)
        typ.isArrayType || (typ.isObjectType && !isImmutable(typ.asObjectType.fqn))
      })
      .map(num => if (num == 0) param0 else param1)

    // TODO improvement: If a field or array of a mutable object is read-accessed,
    // the returned object might not be changed.
    // In that case, we could return true

    // Go through all instructions. If an invocation/putfield/arrayStore involves
    // one of the mutable return params, we cannot be certain that
    // it did not change as a consequence of the method call
    if (!mutableReturnParams.isEmpty) {
      foreachPCWithOperands(domain)(method.body.get, result.operandsArray) {
        (pc, instruction, ops) =>
          instruction match {
            // cannot change the object
            case FieldReadAccess(_, _, fieldType) if fieldType.isBaseType
              || (fieldType.isObjectType && isImmutable(fieldType.asObjectType.fqn)) =>
            case _ : ArrayLoadInstruction if {
              val domain.DomainArrayValue(av) = ops(1)
              val innerType = av.theUpperTypeBound.componentType
              innerType.isBaseType || (innerType.isObjectType && isImmutable(innerType.asObjectType.fqn))
            } =>
              
            // all dangerous cases
            case _: MethodInvocationInstruction | _: FieldAccess | _: ArrayAccessInstruction =>
              println(s"PC $pc, i $instruction, ops $ops")
              ops.foreach { op =>
                op match {
                  case Origins(os) =>
                    if (os.exists { o => mutableReturnParams.contains(o) }) {
                      return false
                    }
                  case _ => // no origin means it's a primitive argument
                }
              }
            // not interesting
            case _ =>
          }
      }
    }

    // TODO improvement: recursively check invoked methods

    true
  }

  // XXX OPAL has a built-in mutability checker
  // List of immutable types
  val immutableTypes = Seq(
    "java/lang/String",
    "java/lang/Integer",
    "java/lang/Byte",
    "java/lang/Character",
    "java/lang/Short",
    "java/lang/Boolean",
    "java/lang/Long",
    "java/lang/Double",
    "java/lang/Float",
    "java/lang/StackTraceElement",
    "java/math/BigInteger",
    "java/math/BigDecimal",
    "java/io/File",
    "java/net/URL",
    "java/net/URI",
    "scala/collection/immutable/AbstractMap",
    "scala/collection/immutable/BitSet",
    "scala/collection/immutable/DefaultMap",
    "scala/collection/immutable/HashMap",
    "scala/collection/immutable/HashSet",
    "scala/collection/immutable/IndexedSeq",
    "scala/collection/immutable/IntMap",
    "scala/collection/immutable/Iterable",
    "scala/collection/immutable/LinearSeq",
    "scala/collection/immutable/List",
    "scala/collection/immutable/ListMap",
    "scala/collection/immutable/ListSet",
    "scala/collection/immutable/LongMap",
    "scala/collection/immutable/Map",
    "scala/collection/immutable/MapLike",
    "scala/collection/immutable/MapProxy",
    "scala/collection/immutable/Nil",
    "scala/collection/immutable/NumericRange",
    "scala/collection/immutable/PagedSeq",
    "scala/collection/immutable/Queue",
    "scala/collection/immutable/Range",
    "scala/collection/immutable/Seq",
    "scala/collection/immutable/Set",
    "scala/collection/immutable/SetProxy",
    "scala/collection/immutable/SortedMap",
    "scala/collection/immutable/SortedSet",
    "scala/collection/immutable/Stack",
    "scala/collection/immutable/Stream",
    "scala/collection/immutable/StreamIterator",
    "scala/collection/immutable/StreamView",
    "scala/collection/immutable/StreamViewLike",
    "scala/collection/immutable/StringLike",
    "scala/collection/immutable/StringOps",
    "scala/collection/immutable/Traversable",
    "scala/collection/immutable/TreeMap",
    "scala/collection/immutable/TreeSet",
    "scala/collection/immutable/Vector",
    "scala/collection/immutable/VectorBuilder",
    "scala/collection/immutable/VectorIterator",
    "scala/collection/immutable/WrappedString")

  /**
   * Checks if a type is known to be immutable
   */
  def isImmutable(name: String) = {
    immutableTypes.contains(name)
  }

  /**
   * Specially crafted domain with capabilities to determine if a function
   * is a select function.
   */
  class IsSelectDomain[Source](
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
      with GlobalLogContextProvider
      with RecordDefUse {
    def classHierarchy = org.opalj.br.ClassHierarchy(Seq(classFile))
  }
}