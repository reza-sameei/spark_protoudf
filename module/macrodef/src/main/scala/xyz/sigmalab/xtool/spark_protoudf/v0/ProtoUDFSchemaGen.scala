package xyz.sigmalab.xtool.spark_protoudf.v0

import scala.language.experimental.macros
import scala.reflect.macros.blackbox.Context

object ProtoUDFSchemaGen {

    def shcemaOf[T <: com.google.protobuf.GeneratedMessageV3]: Unit = macro shcemaOf_impl_v1[T]

    def shcemaOf_impl_v1[
        T <: com.google.protobuf.GeneratedMessageV3 : c.WeakTypeTag
    ](c: Context) = {

        def debug(msg: String) = {
            // c.info(c.enclosingPosition, msg, false)
            println(msg)
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
            val name = m.name.encodedName.toString.replace("_FIELD_NUMBER","")
            debug(s"Static Term: ${name}, ${m.name}, ${m.typeSignature}, ${m.getClass.getName}, ${m}, ${m.owner}")
            name
        }.toSet

        require(originSym.name.isTypeName)

        val typeName = s"${originSym.name.encodedName.toString}OrBuilder"

        val view = originType.baseClasses.find {_.fullName endsWith typeName}.get

        debug(s"Info: ${view.name} / ${view.fullName}")

        val mems = view.info.decls.flatMap { m =>
            if (!m.isMethod) None
            else {
                val name = m.name.encodedName.toString
                val upperCase = name.toUpperCase
                if (!upperCase.startsWith("GET")) None
                else {
                    val temp = upperCase.substring(3)
                    val matches = names.filter { _.startsWith(temp) }
                    if (matches.size == 0) None
                    else if(matches.size != 1) c.abort(???, ???)
                    else Some(m, m.name.encodedName.toString.substring(3))
                }
            }
            /*withFilter{ m => m.isMethod && m.name.encodedName.toString.toLowerCase.contains() }.foreach { m =>
                debug(s"Member: ${m.name}, ${m.typeSignature}")
            }*/
        }





        println("======================")
        val fileds = mems.map { case (m,n) =>
                m.info.paramLists match {
                    case List(List()) => (n, "SINGLE",m.info.resultType, m)
                    case List(i) :: Nil if i.typeSignature =:= typeOf[Int] => (n, "SEQ", m.info.resultType, m)
                    case what => c.abort(c.enclosingPosition, s"${what}")
                }
        }.toList.sortBy(_._1)
        /*mems.map { case (m,n) =>
            debug(s" >>> ${n}   | ${m.name}, ${m.typeSignature}, ${m.getClass.getName}, ${m}, ${m.owner}")
            m.info.resultType match {
                case tpe if tpe <:< typeOf[com.google.protobuf.GeneratedMessageV3] => ???
                case tpe if tpe <:< typeOf[java.util.List[_]] => ???
                    tpe.typeParams

                case tpe if tpe =:= typeOf[Int]
                    || tpe =:= typeOf[Long]
                    || tpe =:= typeOf[Float]
                    || tpe =:= typeOf[Double]
                    || tpe =:= typeOf[Double]
                    || tpe =:= typeOf[String] => (n,tpe,m)
            }

        }*/

        q""
    }


}
