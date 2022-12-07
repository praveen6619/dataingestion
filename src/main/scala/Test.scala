import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.tupol.spark.SparkApp
import org.tupol.spark.io.DataSinkConfiguration
import org.tupol.spark.io.sources._
import org.tupol.spark.io.implicits._
import org.tupol.configz.Configurator
import org.tupol.utils.implicits._

object Test extends SparkApp [TestContext,Unit]{

    override def createContext(config: Config): TestContext =
        TestContext(config).get

    override def run(implicit spark: SparkSession, context: TestContext): Unit = {
        print(context.input)
        val input_configStr=context.input
        val output_configStr=context.output
            val input_config = ConfigFactory.parseString(input_configStr)
            val input_converterConfig = SourceConfiguration.extract(input_config)
            val df1=spark.source(input_converterConfig).read
            //df1.collect()
            val output_config = ConfigFactory.parseString(output_configStr)
            val output_converterConfig = DataSinkConfiguration.extract(output_config)
            //val df1=spark.source(converterConfig).read
            df1.sink(output_converterConfig).write
    }
}

case class TestContext(input:String,output:String)

object TestContext extends Configurator[TestContext] {
    import com.typesafe.config.Config
    import org.tupol.utils.config._
    import scalaz.ValidationNel
    import scalaz.syntax.applicative._

    override def validationNel(config: Config): ValidationNel[Throwable, TestContext] = {
        config.extract[String]("input") |@|
          config.extract[String]("output") apply
          TestContext.apply
    }
}

