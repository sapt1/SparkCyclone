import java.nio.file.{Files, Path, Paths}

object CMakeBuilder {

  val nccPath = Paths.get("/opt/nec/ve/bin/ncc")

  val hasNcc = Files.exists(nccPath)

  sealed trait Builder {
    def prepare(targetPath: Path): List[String]

    def buildLibrary(targetPath: Path): List[String]

    def resolveNative(targetPath: Path): Path
  }

  val isWin = System.getProperty("os.name").toLowerCase.contains("win")

  object Builder {
    def default: Builder = {
      val os = System.getProperty("os.name").toLowerCase
      os match {
        case _ if os.contains("win") => WindowsBuilder
        case _ if os.contains("lin") => LinuxBuilder
        case _                       => MacOSBuilder
      }
    }

    object WindowsBuilder extends Builder {
      override def prepare(targetPath: Path) =
        List("C:\\Program Files\\CMake\\bin\\cmake", "-A", "x64", targetPath.toString)

      override def buildLibrary(targetPath: Path) =
        List("C:\\Program Files\\CMake\\bin\\cmake", "--build", targetPath.getParent.toString)

      override def resolveNative(targetPath: Path): Path =
        targetPath.getParent.resolve("Debug").resolve(s"${libN}.dll")
    }

    object LinuxBuilder extends Builder {
      override def prepare(targetPath: Path): List[String] = List("cmake", targetPath.toString)

      override def buildLibrary(targetPath: Path): List[String] =
        List("make", "-C", targetPath.getParent.toString)

      override def resolveNative(targetPath: Path): Path =
        targetPath.getParent.resolve(s"lib${libN}.so")
    }

    object MacOSBuilder extends Builder {
      override def prepare(targetPath: Path) = List("cmake", targetPath.toString)

      override def buildLibrary(targetPath: Path) =
        List("make", "-C", targetPath.getParent.toString)

      override def resolveNative(targetPath: Path): Path =
        targetPath.getParent.resolve(s"lib${libN}.dylib")
    }
  }

  val libN = "cyclone-host"
}
