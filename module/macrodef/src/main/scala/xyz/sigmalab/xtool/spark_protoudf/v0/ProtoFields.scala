package xyz.sigmalab.xtool.spark_protoudf.v0

import org.apache.spark.sql.types._
import scala.language.experimental.macros
import scala.reflect.macros.blackbox.Context

object ProtoFields {

    case class Data(name: String, isRepeatable: Boolean, resultDesc: String) {
        override def toString = f"${if(isRepeatable) "repeated" else "single"}%8s ${name} : ${resultDesc}"
    }

    def of[T <: com.google.protobuf.GeneratedMessageV3]: Seq[Data] = macro of_impl_v1[T]

    def of_impl_v1[
        T <: com.google.protobuf.GeneratedMessageV3 : c.WeakTypeTag
    ](c: Context) = {

        def debug(msg : String) = {
            // c.info(c.enclosingPosition, msg, false)
            // println(msg)
        }

        import c.universe._

        val originTag = weakTypeTag[T]

        val originType = weakTypeOf[T]

        if (
            originType.baseClasses.find(_.fullName == "com.google.protobuf.GeneratedMessageV3").isEmpty
        ) c.abort(c.enclosingPosition, s"Invalid class: ${originType}")

        val originSym = originType.typeSymbol

        if (
            !originSym.isClass
                || !originSym.isPublic
        ) c.abort(c.enclosingPosition, s"Invalid class: ${originType}")

        val names = originType.companion.members.withFilter { m =>
            m.isTerm && !m.isMethod && m.name.encodedName.toString.endsWith("_FIELD_NUMBER") // && m.typeSignature =:= typeOf[Int]
        }.map { m =>
            val name = m.name.encodedName.toString.replace("_FIELD_NUMBER", "")
            debug(s"Static Term: ${name}, ${m.name}, ${m.typeSignature}, ${m.getClass.getName}, ${m}, ${m.owner}")
            name
        }.toSet

        require(originSym.name.isTypeName)

        val typeName = s"${originSym.name.encodedName.toString}OrBuilder"

        val view = originType.baseClasses.find { _.fullName endsWith typeName }.get

        debug(s"Info: ${view.name} / ${view.fullName}")

        val qs = view.info.decls.flatMap { m =>
            if ( !m.isMethod ) None
            else {
                val name = m.name.encodedName.toString
                val upperCase = name.toUpperCase
                if ( !upperCase.startsWith("GET") ) None
                else {
                    val temp = upperCase.substring(3)
                    val matches = names.filter { _.startsWith(temp) }
                    if ( matches.size == 0 ) None
                    else if ( matches.size != 1 ) c.abort(???, ???)
                    else Some(m, m.name.encodedName.toString.substring(3))
                }
            }

        }.map { case (m,n) =>
            m.info.paramLists match {
                case List(List()) => (n, q"Data($n, false, ${m.info.resultType.toString})")
                case List(i) :: Nil if i.typeSignature =:= typeOf[Int] => (n, q"Data($n, true, ${m.info.resultType.toString})")
                case what => c.abort(c.enclosingPosition, s"${what}")
            }
        }.toList.sortBy(_._1)

        val tree =
            q"""
                import xyz.sigmalab.xtool.spark_protoudf.v0.ProtoFields.Data
                List(..${qs.map(_._2)})
                """

        println(showCode(tree))

        tree
    }
}
