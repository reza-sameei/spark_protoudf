package xyz.sigmalab.xtool.spark_protoudf.v0

import org.scalatest._
import xyz.sigmalab.xtool.spark_protoudf.Protobuf
import org.apache.spark.sql.types._

class TestSuite extends FlatSpec with MustMatchers {

    it must "print fields" in {
        ProtoFields.printSchemaOf[Protobuf.Person]

        ProtoFields.printSchemaOf[Protobuf.SimplePerson]
    }

    it must "not compile bacause of a complext-type" in {

        // "val a: String = 1" mustNot compile

        """
            import xyz.sigmalab.xtool.spark_protoudf.v0.ProtoFields
            import xyz.sigmalab.xtool.spark_protoudf.Protobuf
            ProtoFields.schemaOf[Protobuf.Person]
        """ mustNot compile
    }

    it must "compile" in {

        val group = ProtoFields.schemaOf[Protobuf.SimpleGroup]
        val expt = StructType(StructField("Name", StringType) :: Nil)
        info(expt.toString)
        info(group.toString)
        group mustEqual expt

        /*val struct = ProtoFields.schemaOf[Protobuf.SimplePerson]
        info(struct.toString)*/
    }
}
