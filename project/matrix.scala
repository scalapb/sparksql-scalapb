case class Spark(sparkVersion: String) extends sbt.VirtualAxis.WeakAxis {
  def majorVersion = sparkVersion.split('.')(0)

  def minorVersion = sparkVersion.split('.')(1)

  def idSuffix = s"spark${majorVersion}${minorVersion}"

  def directorySuffix = idSuffix
}

case class ScalaPB(scalapbVersion: String) extends sbt.VirtualAxis.WeakAxis {
  def majorVersion = scalapbVersion.split('.')(0)

  def minorVersion = scalapbVersion.split('.')(1)

  def idSuffix = s"scalapb${majorVersion}_${minorVersion}"

  def directorySuffix = idSuffix
}
