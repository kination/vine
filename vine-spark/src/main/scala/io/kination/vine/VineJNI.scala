package io.kination.vine

object VineJNI {
  System.loadLibrary("/Users/kination/workspace/public/vine/vine-core/target/release/libvine_core")

  @native def readDataFromVine(path: String): String
  @native def writeDataToVine(path: String, data: String): Unit
}
