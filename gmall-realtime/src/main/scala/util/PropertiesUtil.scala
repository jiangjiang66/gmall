package util
import java.io.InputStream
import java.util.Properties

object PropertiesUtil {
  private val is: InputStream = ClassLoader.getSystemResourceAsStream("config.properties")
  private val properties = new Properties()
  properties.load(is)
  def getProperty(propertyName: String): String = properties.getProperty(propertyName)

  def main(args: Array[String]): Unit = {
    println(getProperty("kafka.broker.list"))
  }
}
