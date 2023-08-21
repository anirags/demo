package com.sundogsoftware.spark
import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.amazon.deequ.analyzers._
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.profiles.ColumnProfilerRunner
import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
object TryThings {

  def main(args:Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("TryThings")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read.option("header",true).option("sep","~").csv("src/resources/org/WHA_PROVIDER_20230724215255.dat")

    val analyzerResult:AnalyzerContext = { AnalysisRunner
      //data to run analysis on
      .onData(df)
      //define analyzers to compute metrics
      .addAnalyzer(Size())
      .addAnalyzer(Completeness("ENTITY_ID"))
      .addAnalyzer(ApproxCountDistinct("ENTITY_ID"))
      .addAnalyzer(Minimum("EFFECTIVE_DT"))
      .addAnalyzer(Mean("EFFECTIVE_DT"))
      .addAnalyzer(Maximum("EFFECTIVE_DT"))
      .addAnalyzer(Entropy("EFFECTIVE_DT"))
      .run()
    }

    //retrieve successfully computed metrics
    val statistics = successMetricsAsDataFrame(spark,analyzerResult)
//    statistics.show(false)

    import spark.implicits._
    val suggestionResult = {ConstraintSuggestionRunner()
    .onData(df)
    .addConstraintRules(Rules.DEFAULT)
    .run()
    }
    //get suggestions
    val constraintsSuggestions = suggestionResult.constraintSuggestions.flatMap{
      case (column,suggestions) =>
        suggestions.map{constraints => (column,constraints.description,constraints.codeForConstraint)}
    }.toSeq.toDS()

//    constraintsSuggestions.show(100,false)

    val verificationResult:VerificationResult = { VerificationSuite()
      // data to run on verification
      .onData(df)
      //define data quality check
      .addCheck(
        Check(CheckLevel.Error, "Data Quality")
          .hasCompleteness("DOB", _ >= 0.69, Some("It should be above 0.69!"))
          .isContainedIn("ENTITY_TYPE", Array("PROVIDER", "ORGANIZATION"))
          .isNonNegative("ENTITY_ID")
          .isComplete("ENTITY_ID")
          .isContainedIn("GENDER_CD", Array("M", "F"))
          .isComplete("EFFECTIVE_DT")
          .isComplete("ENTITY_TYPE")
      )
      .run()
    }
    val resultDf = checkResultsAsDataFrame(spark,verificationResult)
//    resultDf.show(false)

    val result = ColumnProfilerRunner()
      .onData(df)
      .run()

    result.profiles.foreach { case (colName,profile) =>
      println(s"Column : $colName \n" +
      s"completeness : ${profile.completeness}\n" +
      s"approximate number of distinct value : ${profile.approximateNumDistinctValues}\n" +
      s"datatype : ${profile.dataType}\n")

    }

  }


}

