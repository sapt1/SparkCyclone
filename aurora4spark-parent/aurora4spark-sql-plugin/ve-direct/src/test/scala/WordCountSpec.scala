import CountStringsCSpec.withArrowStringVector
import com.nec.WordCount
import org.scalatest.freespec.AnyFreeSpec

final class WordCountSpec extends AnyFreeSpec {
  "JVM word count works" in {
    withArrowStringVector(Seq("hello", "test", "hello")) { vcv =>
      assert(WordCount.wordCountJVM(vcv) == Map("hello" -> 2, "test" -> 1))
    }
  }
}
