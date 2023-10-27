package io.github.lineageco.splinecustomhttpdispatcher

import org.apache.commons.configuration.Configuration
import org.apache.spark.internal.Logging
import za.co.absa.commons.config.ConfigurationImplicits.ConfigurationRequiredWrapper
import za.co.absa.commons.version.Version
import za.co.absa.spline.harvester.dispatcher.modelmapper.ModelMapper
import scala.util.control.NonFatal
//import za.co.absa.spline.harvester.exception.SplineInitializationException
import za.co.absa.spline.producer.model.{ExecutionEvent, ExecutionPlan}
import za.co.absa.spline.harvester.dispatcher.LineageDispatcher


/**
 * AzureBlobLineageDispatcherConfig is responsible for sending the lineage data to Azure Blob Storage through the producer API
 */

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.nio.file.{Files,Paths}
import io.github.lineageco.splinecustomhttpdispatcher.AzureBlobLineageDispatcher._
import za.co.absa.spline.harvester.json.HarvesterJsonSerDe.impl._

class AzureBlobLineageDispatcher(filePath: String,
                                 fileNameKey: String,
                                 apiVersion: Version)
  extends LineageDispatcher
    with Logging {


  def this(conf: Configuration) = this(
    filePath = conf.getRequiredString(FilePath),
    fileNameKey = conf.getRequiredString(FileNameKey),
    apiVersion = Version.asSimple(conf.getRequiredString(ApiVersion))
  )
  logInfo(s"Using File Path: ${filePath}")
  logInfo(s"Using file name key: ${fileNameKey}")
  logInfo(s"Using api version: ${apiVersion}")

  override def name = "Blob"

  private val modelMapper = ModelMapper.forApiVersion(apiVersion)

  override def send(plan: ExecutionPlan): Unit = {
    for (execPlanDTO <- modelMapper.toDTO(plan)) {
      persistJsonInDbfs(execPlanDTO.toJson, filePath,fileNameKey)
    }
  }

  override def send(event: ExecutionEvent): Unit = {
    for (eventDTO <- modelMapper.toDTO(event)) {
//      persistJsonInDbfs(Seq(eventDTO).toJson, filePath,fileNameKey)
      logInfo(s"event is: B")
    }
  }

  private def persistJsonInDbfs(json: String, dbfsFilePath: String, fileNamePattern: String): Unit = {

    // Get the current timestamp
    val currentDateTime = LocalDateTime.now()

    // Define the file timestamp formatter for the desired format
    val fileTimestampFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")

    // Format the file timestamp as a string
    val fileTimeStamp = currentDateTime.format(fileTimestampFormatter)

    val fileNameWithDateTime = s"${fileNamePattern}_${fileTimeStamp}.json"

    // Define the directory date formatter for the desired format
    val dirDateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

    // Format the directory date as a string
    val dirDate = currentDateTime.format(dirDateFormatter)

    val completeDirPath = s"${dbfsFilePath}/date=${dirDate}"

//    logInfo(s"sendJson lookslike : \n${json.asPrettyJson}")

    // Define the path to the local directory and the file name
    val directoryPath = Paths.get(completeDirPath)
    val filePath = directoryPath.resolve(fileNameWithDateTime)


    try {

      // Create the directory if it doesn't exist
      if (!Files.exists(directoryPath)) {
        Files.createDirectories(directoryPath)
      }

      // Write the JSON data to the file
      Files.write(filePath, json.getBytes("UTF-8"))

    } catch {
      case NonFatal(e) =>
        throw new RuntimeException(s"Cannot write data to ${filePath}", e)
    }
  }

}

object AzureBlobLineageDispatcher {
  val FilePath = "databricks.filepath"
  val FileNameKey = "filename.pattern"
  val ApiVersion = "apiVersion"
}

