package scalapb

import com.google.protobuf.Descriptors._
import com.google.protobuf.compiler.PluginProtos.{ CodeGeneratorRequest, CodeGeneratorResponse }

import scala.collection.JavaConverters._
import scalapb.compiler.{ DescriptorPimps, FunctionalPrinter, GeneratorParams }

class UdtGenerator(flatPackage: Boolean = false) extends protocbridge.ProtocCodeGenerator with DescriptorPimps {
  val params = GeneratorParams(flatPackage = flatPackage)

  def run(request: CodeGeneratorRequest): CodeGeneratorResponse = {
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
    b.build
  }

  def allEnums(f: FileDescriptor): Seq[EnumDescriptor] = {
    f.getEnumTypes.asScala ++ f.getMessageTypes.asScala.flatMap(allEnums)
  }

  def allEnums(f: Descriptor): Seq[EnumDescriptor] = {
    f.getEnumTypes.asScala ++ f.nestedTypes.flatMap(allEnums)
  }

  def udtName(e: EnumDescriptor) = e.scalaTypeName.replace(".", "__")

  def generateEnum(fp: FunctionalPrinter, e: EnumDescriptor): FunctionalPrinter = {
    fp.add(
      s"class ${udtName(e)} extends _root_.org.apache.spark.scalapb_hack.GeneratedEnumUDT[${e.scalaTypeName}]")
  }

  def generateFile(fileDesc: FileDescriptor): CodeGeneratorResponse.File = {
    val b = CodeGeneratorResponse.File.newBuilder()
    b.setName(s"${fileDesc.scalaDirectory}/${fileDesc.fileDescriptorObjectName}Udt.scala")
    val fp = FunctionalPrinter()
      .add(s"package ${fileDesc.scalaPackageName}")
      .add("")
      .add(s"object ${fileDesc.fileDescriptorObjectName}Udt {")
      .indent
      .print(allEnums(fileDesc))(generateEnum)
      .add("def register(): Unit = { } // actual work happens at the constructor.")
      .add("")
      .print(fileDesc.getDependencies.asScala)(
        (fp, dep) => fp.add(s"${dep.scalaPackageName}.${dep.fileDescriptorObjectName}Udt.register()")
      )
      .print(allEnums(fileDesc))(
        (fp, e) => fp.add(
          s"""_root_.org.apache.spark.scalapb_hack.GeneratedEnumUDT.register(classOf[${e.scalaTypeName}].getName, classOf[${fileDesc.scalaPackageName}.${fileDesc.fileDescriptorObjectName}Udt.${udtName(e)}].getName)"""))
      .outdent
      .add("}")

      b.setContent(fp.result)
      b.build
  }
}
