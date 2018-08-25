package xyz.sigmalab.xtool.spark_protoudf.v0

class SampleCode {

    type BaseType = com.google.protobuf.GeneratedMessageV3

    val builder =
        xyz.sigmalab.xtool.spark_protoudf.Protobuf.Person.newBuilder()

    builder
        .setInternal(1)
        .setNickName("Reza")
        .addEmail("reza.samee@gmail.com")


    val value = builder.build


}
