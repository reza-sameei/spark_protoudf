package xyz.sigmalab.xtool.spark_protoudf.v0

import org.scalatest._

class TestSuite extends FlatSpec with MustMatchers {

    it must "?" in {

        import xyz.sigmalab.xtool.spark_protoudf.Protobuf

        val fields = ProtoFields.of[Protobuf.Person]

        fields.foreach { i => info(i.toString) }

    }

}
