package xyz.sigmalab.xtool.spark_protoudf.v0

import org.apache.spark.sql.types._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.language.experimental.macros
import scala.reflect.macros.blackbox.Context
import scala.collection.mutable.{HashMap, ListBuffer, Stack}

object ProtoFields {

    def schemaOf_impl[T <: com.google.protobuf.GeneratedMessageV3](
        c: Context
    )(
        implicit tag: c.WeakTypeTag[T]
    ): c.Tree = {
        new Helper(c).schemaOf(tag.tpe).asInstanceOf[c.Tree]
    }

    def schemaOf[T <: com.google.protobuf.GeneratedMessageV3]: DataType = macro schemaOf_impl[T]

    def printSchemaOf_impl[T <: com.google.protobuf.GeneratedMessageV3](
        c: Context
    )(
        implicit tag: c.WeakTypeTag[T]
    ): c.Tree = {
        import c.universe._
        new Helper(c).print(tag.tpe)
        q""
    }
    def printSchemaOf[T <: com.google.protobuf.GeneratedMessageV3]: Unit = macro printSchemaOf_impl[T]

}


private[v0] class Helper[C <: Context](protected val c: C) {

    import c.universe._

    case class FieldName(name: String, code: Int)

    sealed trait TypeKind { def name: String; override def toString = name }
    object Primitive extends TypeKind { override val name = "Primitive" }
    object Enum extends TypeKind { override val name = "Enum" }
    object POJO extends TypeKind { override val name = "POJO"}

    case class FieldDesc(
        name: String,
        tpe: c.Type,
        typeAsString: String,
        isRepeatable: Boolean, kind: TypeKind
    )

    case class KindRef(name: String, tpe: c.Type, kind: TypeKind)

    case class EnumVal(name: String, value: Int, tpeAsString: String, tpe: c.Type)


    trait KindDesc {
        def name: String
        def tpe: c.Type
    }
    case class TypeDesc(name: String, tpe: c.Type, fields: Seq[FieldDesc]) extends KindDesc
    case class EnumDesc(name: String, tpe: c.Type, values: Seq[EnumVal]) extends KindDesc

    // checkTypeAsGeneratedMessageV3(originType, "#1, CheckType, GeneratedMessageV3")

    protected def checkTypeAsGeneratedMessageV3(originType: c.Type, error: String): Unit = {

        val originSym = originType.typeSymbol

        val originName = originSym.name.encodedName.toString

        if (
            !originSym.isClass || !originSym.isPublic ||
                !(originType <:< typeOf[com.google.protobuf.GeneratedMessageV3])
        ) c.abort(c.enclosingPosition, s"Error: ${error}")

    }

    protected def generalError(msg: String) = c.abort(c.enclosingPosition, s"Error: ${msg}")

    protected def generalDebug(msg: String) = c.info(c.enclosingPosition, msg, false)

    protected def namesOfPOJOFields(
        originType: c.Type
    ): Iterable[FieldName] = {

        val originSym = originType.typeSymbol

        val originName = originSym.name.encodedName.toString

        originType.companion.members.flatMap {
            case m: TermSymbol if m.isStatic && m.isStable && m.isVal => Some(m.asTerm)
            case _ => None
        }.withFilter { t =>
            t.name.encodedName.toString.endsWith("_FIELD_NUMBER") &&
                t.typeSignature.typeConstructor =:= typeOf[Int] // t.typeSignature <:< typeOf[Int]
        }.map{ t =>
            val name = t.name.encodedName.toString.replace("_FIELD_NUMBER", "")
            val code = t.typeSignature.toString.replace("Int(","").replace(")","").toInt
            FieldName(name, code)
        }
    }

    protected def builderOfPOJO(
        originType: c.Type
    ): c.Type = {

        val originSym = originType.typeSymbol
        val originName = originSym.name.encodedName.toString
        val builderName = s"${originName}OrBuilder"

        originType.baseClasses.find { _.fullName endsWith builderName }.get match {
            case sym: TypeSymbol => sym.typeSignature
            case what => generalError(s"#2, BuilderOf: ${originName}")
        }
    }

    protected def descsOfPOJOFileds(
        originType: c.Type, builderType: c.Type, names: Seq[FieldName]
    ): Iterable[FieldDesc] = {

        val originSym = originType.typeSymbol

        val originName = originSym.name.encodedName.toString

        val builderSym = builderType.typeSymbol

        val builderName = builderSym.name.encodedName.toString

        val namesIndex = names.map{_.name}.toSet

        builderType.decls.flatMap { m =>
            if ( !m.isMethod ) None
            else {
                val name = m.name.encodedName.toString
                val upperCase = name.toUpperCase
                if ( !upperCase.startsWith("GET") ) None
                else {
                    val temp = upperCase.substring(3)
                    val matches = namesIndex.filter { _.startsWith(temp) }
                    if ( matches.size == 0 ) None
                    else if ( matches.size != 1 ) generalError(s"#3, Getter, Of: ${name}, In: ${originName}")
                    else Some(m) // method-getter-ref
                }
            }

        }.map { methodSym =>
            val name = methodSym.name.encodedName.toString.substring(3)
            val isRepeatable = methodSym.info.paramLists match {
                case List(List()) => false
                case List(i) :: Nil if i.typeSignature =:= typeOf[Int] => true
                case what => generalError(s"#4, Unexpected Getter, Of: ${name}, In: ${originName}")
            }
            val tpe = methodSym.typeSignature.resultType
            val tpeAsString = tpe match {
                case it if it <:< typeOf[Int] => "Int"
                case it if it <:< typeOf[Long] => "Long"
                case it if it <:< typeOf[Boolean] => "Boolean"
                case it if it <:< typeOf[Double] => "Double"
                case it if it <:< typeOf[Float] => "Float"
                case it if it <:< typeOf[String] => "String"
                case it => it.toString
            }

            val kind = tpeAsString match {
                case "Int" | "Long" | "Boolean" | "Double" | "Float" | "String" => Primitive
                case _ if tpe.baseClasses.find(_.fullName == "java.lang.Enum").isDefined => Enum
                case _ => POJO
            }

            FieldDesc(name, tpe, tpeAsString, isRepeatable, kind)
        }
    }

    protected def descsOfPOJOFileds(
        originType: c.Type
    ): Iterable[FieldDesc] = {
        val names = namesOfPOJOFields(originType).toSeq
        val builder = builderOfPOJO(originType)
        descsOfPOJOFileds(originType, builder, names)
    }

    protected def descsOfEnumVals(
        originType: c.Type
    ): Iterable[EnumVal] = Seq()

    @tailrec protected final def explore(
        unexplored: HashMap[String, KindRef],
        explored: HashMap[String, KindDesc]
    ): HashMap[String, KindDesc]  = unexplored.headOption match {

        case None =>
            explored

        case Some((_,KindRef(name, tpe, POJO))) =>
            val fields = descsOfPOJOFileds(tpe).toSeq
            fields.foreach {
                case f if f.kind == Primitive => // Ignore
                case f if f.typeAsString == name => // Ignore
                case f if explored.contains(f.typeAsString) => // Ignore
                case f if unexplored.contains(f.typeAsString) => // Ignore
                case f => unexplored(f.typeAsString) = KindRef(f.typeAsString, f.tpe, f.kind)
            }
            unexplored.remove(name)
            explored(name) = TypeDesc(name, tpe, fields)
            explore(unexplored, explored)

        case Some((_,KindRef(name, tpe, Enum))) =>
            val vals = descsOfEnumVals(tpe).toSeq
            explored(name) = EnumDesc(name, tpe, vals)
            explore(unexplored.tail, explored)

        case Some((_,KindRef(name, tpe, Primitive))) =>
            explore(unexplored.tail, explored) // Imposible
    }

    def describe(originType: C#Type): HashMap[String, KindDesc] = {

        val internal = originType.asInstanceOf[c.Type]
        val name = internal.typeSymbol.fullName
        val fields = descsOfPOJOFileds(internal).toSeq

        explore(
            HashMap(name -> KindRef(name, internal, POJO)),
            HashMap.empty
        )
    }

    protected final def isComplex(all: mutable.HashMap[String, KindDesc]): Boolean = {
        all.values.flatMap {
            case t: TypeDesc => Some(t)
            case _ => None
        }.foldLeft(false){
            case (true, _) => true
            case (false, t) => isComplex(t, all, Stack(t.name))
        }
    }

    protected final def isComplex(
        t: TypeDesc,
        all: mutable.HashMap[String, KindDesc],
        stack: Stack[String]
    ): Boolean = {
        t.fields.foldLeft(false) {
            case (true, _) => true
            case (false, f) if f.kind != POJO => false
            case (false, f) if stack.contains(f.typeAsString) =>
                generalDebug(s"COMPLEX_TYPE(${t.name}) => ${stack.push(f.typeAsString).mkString(" <- ")}")
                true
            case (false, f) => isComplex(
                all(f.typeAsString).asInstanceOf[TypeDesc],
                all, stack.push(f.typeAsString)
            )
        }
    }

    protected def sqltypeOf(primitive: String): DataType = primitive match {
        case "String" => StringType
        case "Int" => IntegerType
        case "Long" => LongType
        case "Float" => FloatType
        case "Double" => DoubleType
        case "Boolean" => BooleanType
    }
    protected def sqltypeOf(tpe: TypeDesc, all: Map[String, KindDesc]) : DataType = {
        tpe.fields.foldLeft(new StructType()) { (buf, f) => f match {

            case fl if fl.kind == Primitive =>
                val t = sqltypeOf(fl.typeAsString)
                buf.add(fl.name, if (fl.isRepeatable) ArrayType(t) else t)

            case fl if fl.kind == Enum =>
                val t = StringType
                buf.add(fl.name, if (fl.isRepeatable) ArrayType(t) else t)

            case fl if fl.kind == POJO =>
                val t = sqltypeOf(all(f.typeAsString).asInstanceOf[TypeDesc],all)
                buf.add(fl.name, if (fl.isRepeatable) ArrayType(t) else t)
        }}
    }

    protected def schemaOf(primitive: String) = primitive match {
        case "String" => TermName("StringType")
        case "Int" => TermName("IntegerType")
        case "Long" => TermName("LongType")
        case "Float" => TermName("FloatType")
        case "Double" => TermName("DoubleType")
        case "Boolean" => TermName("BooleanType")
    }
    protected def schemaOf(tpe: TypeDesc, all: Map[String, KindDesc]): Tree = {

        val list = tpe.fields.foldLeft(new ListBuffer[Tree]) { (code, f) => f match {

            case fl if fl.kind == Primitive =>
                val tree =
                    if (fl.isRepeatable) q"StructField(${fl.name}, ArrayType(${schemaOf(fl.typeAsString)}))"
                    else q"StructField(${fl.name}, ${schemaOf(fl.typeAsString)})"
                code.append(tree)
                code

            case fl if fl.kind == Enum =>
                code.append(
                    if (fl.isRepeatable) q"StructField(${fl.name}, ArrayType(StringType))"
                    else q"StructField(${fl.name}, StringType)"
                )
                code

            case fl if fl.kind == POJO =>
                val tree = schemaOf(all(f.typeAsString).asInstanceOf[TypeDesc],all)
                code.append(
                    if (fl.isRepeatable) q"StructField(${fl.name}, ArrayType(${tree}))"
                    else q"StructField(${fl.name}, ${tree})"
                )
                code
        }}

        q"""
             import org.apache.spark.sql.types._
             StructType(Seq(..$list))
         """
    }

    def schemaOf(originType: C#Type): Tree = {
        val internal = originType.asInstanceOf[Type]
        val name = internal.typeSymbol.fullName
        val all = describe(originType)
        if (isComplex(all)) generalError(s"#5, SchemaOf, ComplextType, ${originType}")
        val allTypes = all.toMap
        val mainType = all(name).asInstanceOf[TypeDesc]
        schemaOf(mainType, allTypes)
    }

    def print(originType: C#Type) : Unit = {

        val internal = originType.asInstanceOf[Type]
        val name = internal.typeSymbol.fullName
        val all = describe(originType)

        all.values.foreach { t =>
            println("="*40)
            t match {
                case t: TypeDesc =>
                    println(s"POJO: ${t.name}")
                    t.fields.foreach { f => println(s"    | ${f}")}
                case e: EnumDesc =>
                    println(s"Enum: ${t.name}")
                    e.values.foreach { v => println(s"    | ${v}")}
            }

        }

        println("="*40)

        val complex = isComplex(all)
        if (complex ) println("COMPLEX TYPE") else println("SIMPLE TYPE!")

        println("")

        if (!complex) {
            val allTypes = all.toMap
            val mainType = all(name).asInstanceOf[TypeDesc]

            val sparksqlType = sqltypeOf(mainType, allTypes)
            println(s"SparkSQL: ${sparksqlType}")

            val code = schemaOf(mainType, allTypes)
            println(showCode(code))
        }

        println("")

    }


}