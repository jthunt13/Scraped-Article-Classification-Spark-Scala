import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{SparkSession,DataFrame,Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.classification.{LogisticRegression,LogisticRegressionModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, VectorAssembler,VectorIndexer, StringIndexer,StopWordsRemover,HashingTF, Tokenizer}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.log4j.{Level, Logger}

import java.io.File
import java.nio.file.{Files}

object wordPipeline {

    def main(args: Array[String]): Unit = {
        //define arguments
        val dataDirectory = args(0)
        val trainFolder = dataDirectory + "/train"
        val validationFolder = dataDirectory + "/validate"
        val testFolder = dataDirectory + "/test"

        val conf = new SparkConf().setAppName("wordCount")
        val sc = new SparkContext(conf)
        val rootLogger = Logger.getRootLogger()
        rootLogger.setLevel(Level.ERROR)

        var spark = SparkSession
            .builder()
            .appName("docClassifier")
            .getOrCreate()

        // get training data
        val train = getData(trainFolder,spark,sc,dataDirectory)
        // get validation data
        val validation = getData(validationFolder,spark,sc,dataDirectory)
        // get test data
        val test = getData(testFolder,spark,sc,dataDirectory)
        // initialize list to store results
        var accuracyPerNumFeatures: List[(Int,Double,Double,Double,Double)] = List()
        // check accuracy for multiple number of features
        for{ numOfFeatures <- 10 to 400
            if numOfFeatures % 10 == 0
        } (
            accuracyPerNumFeatures = accuracyPerNumFeatures:+ checkAccuracy(numOfFeatures,train,validation)
        ) // end of for loop

        // create dataFrame from results
        val df = spark.createDataFrame(accuracyPerNumFeatures).toDF("numOfFeatures","lrTrainAcc","lrTestAcc","rfTrainAcc","rfTestAcc")

        //write data frame to csv for further analysis
        df.write.format("csv").save(dataDirectory+"/output")

        // based on csv and plot on report choose 300 num of features

        println("---------------------------------------------------------------")
        println("Final Testing Accuracy")
        println("---------------------------------------------------------------")

        checkAccuracy(300,train,test)

    }//end main

    def checkAccuracy(numOfFeatures: Int,train: DataFrame,test: DataFrame): (Int,Double,Double,Double,Double) = {
        // create the pipelines
        val (lrPipeline,rfPipeline) = makePipelines(numOfFeatures)

        // define the evaluator to use
        val multiClassEval = new MulticlassClassificationEvaluator()
            .setLabelCol("label")
            .setPredictionCol("prediction")

        val paramGrid = new ParamGridBuilder().build()
        // use cross validation to tune parameters for random Forest
        val rfCV = new CrossValidator()
            .setEstimator(rfPipeline)
            .setEvaluator(multiClassEval)
            .setEstimatorParamMaps(paramGrid)
            .setNumFolds(5)

        // build models from pipelines
        val lrModel = lrPipeline.fit(train)
        val rfModel = rfCV.fit(train)

        // get training predictions
        val lrTrainPredictions = lrModel.transform(train)
        val rfTrainPredictions = rfModel.transform(train)
        // get testing predictions
        val lrTestPredictions = lrModel.transform(test)
        val rfTestPredictions = rfModel.transform(test)
        // get prediction accuracies
        val lrTrainAcc = multiClassEval.evaluate(lrTrainPredictions)
        val rfTrainAcc = multiClassEval.evaluate(rfTrainPredictions)
        val lrTestAcc = multiClassEval.evaluate(lrTestPredictions)
        val rfTestAcc = multiClassEval.evaluate(rfTestPredictions)

        //print information to console
        println("\n---------------------------------------------------------------")
        println("Using " + numOfFeatures + " Features")
        println("\nLogistic Regression Train Accuracy: " + lrTrainAcc)
        println("Random Forest Train Accuracy: " + rfTrainAcc)
        println("\nLogistic Regression Test Accuracy: " + lrTestAcc)
        println("Random Forest Test Accuracy: " + rfTestAcc)
        println("---------------------------------------------------------------\n")

        //return tuple of informations
        (numOfFeatures,lrTrainAcc,lrTestAcc,rfTrainAcc,rfTestAcc)
    }// end of checkAccuracy

    def makePipelines(numOfFeatures: Int): (Pipeline,Pipeline) = {
        //creates vanilla pipelines to be tuned by a cross validator
        val labelIndexer = new StringIndexer()
          .setInputCol("stringLabel")
          .setOutputCol("label")

        // pass through tokenizer
        val tokenizer = new Tokenizer()
            .setInputCol("text")
            .setOutputCol("words")

        // pass through hashingTF
        val hashingTF = new HashingTF()
            .setNumFeatures(numOfFeatures)
            .setInputCol("words")
            .setOutputCol("features")

        // make a logistic regression classifier
        val lr = new LogisticRegression()
            .setMaxIter(10)

        // make a RandomForestClassifier
        val rf = new RandomForestClassifier()

        // create  Logistic Regression pipeline
        val lrPipeline = new Pipeline().setStages(
            Array(labelIndexer, tokenizer, hashingTF,lr)
        )

        // create Random Forest pipeline
        val rfPipeline = new Pipeline().setStages(
            Array(labelIndexer, tokenizer, hashingTF,rf)
        )

        (lrPipeline,rfPipeline)
    }// end makePipelines

    def getData(f: String,spark: SparkSession,sc: SparkContext, dd: String): DataFrame = {
        import spark.implicits._

        //define subdirectories to look at
        val subDirs = Array("business","politics","science","sports")

        // initialize empty atricle map list
        var articleMapCollect = collection.mutable.Map.empty[String, String]

        //start a timer
        var now = System.currentTimeMillis

        for(directory <- subDirs){

            // get list of articles in the directory
            var articles = getFiles(f +"/"+ directory)

            // go through each article
            for(article <- articles){

                // get word count for article
                var articleMap = articleMapper(f +"/"+ directory + "/" + article, directory)

                // add article map to list
                articleMapCollect = articleMapCollect ++ articleMap

            }
        } // end directory loop

        val df: DataFrame = articleMapCollect.toList.toDF("text","stringLabel")
        //println("---------------------------------------------------------------")
        //println("Time to create dataframe " + (System.currentTimeMillis() - now) + " ms")
        //println("---------------------------------------------------------------")

        (df)
    } // end getData function

    def articleMapper(f: String,label: String): collection.mutable.Map[String, String] = {

        // read in article contents as single string object
        var words = scala.io.Source.fromFile(f).mkString

        //return words w/ label */
        val map = collection.mutable.Map(words -> label)
        (map)
    } // end article mapper

    def getFiles(dir: String): Array[String] ={

        val directory = new File(dir).list

        (directory)
    }//end getFiles function/method

} // end of Lab3 object
