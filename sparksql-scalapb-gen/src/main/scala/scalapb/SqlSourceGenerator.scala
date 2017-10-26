package scalapb

import com.google.protobuf.Descriptors._
import com.google.protobuf.ExtensionRegistry
import com.google.protobuf.compiler.PluginProtos.{CodeGeneratorRequest, CodeGeneratorResponse}
import com.trueaccord.scalapb.compiler.{DescriptorPimps, FunctionalPrinter, GeneratorParams}

import scala.collection.JavaConverters._
import scalapbshade.v0_6_6.com.trueaccord.scalapb.Scalapb

class SqlSourceGenerator(flatPackage: Boolean = false) extends protocbridge.ProtocCodeGenerator with DescriptorPimps {
  val params = GeneratorParams(flatPackage = flatPackage)


  override def run(req: Array[Byte]): Array[Byte] = {
    val registry = ExtensionRegistry.newInstance()
    Scalapb.registerAllExtensions(registry)
    val request = CodeGeneratorRequest.parseFrom(req, registry)

    val b = CodeGeneratorResponse.newBuilder

    val fileDescByName: Map[String, FileDescriptor] =
      request.getProtoFileList.asScala.foldLeft[Map[String, FileDescriptor]](Map.empty) {
        case (acc, fp) =>
          val deps = fp.getDependencyList.asScala.map(acc)
          acc + (fp.getName -> FileDescriptor.buildFrom(fp, deps.toArray))
      }


    request.getFileToGenerateList.asScala.foreach {
      name =>
        val fileDesc = fileDescByName(name)
        val responseFile = generateFile(fileDesc)
        b.addFile(responseFile)
    }
    b.build.toByteArray
  }

  def generateFile(fileDesc: FileDescriptor): CodeGeneratorResponse.File = {
    val b = CodeGeneratorResponse.File.newBuilder()
    b.setName(s"${fileDesc.scalaDirectory}/${fileDesc.fileDescriptorObjectName}DefaultSource.scala")
    val fp = FunctionalPrinter()
      .add(s"package ${fileDesc.scalaPackageName}")
      .add("")
      .print(fileDesc.getMessageTypes.asScala)((fp, m) => fp.add(s"""class ${m.nameSymbol}ProtoSource extends _root_.com.trueaccord.scalapb.spark.proto.DefaultSource[${fileDesc.scalaPackageName}.${m.nameSymbol}] { override def shortName = "${fileDesc.scalaPackageName}.${m.nameSymbol}" }"""))
      b.setContent(fp.result)
      b.build
  }
}
