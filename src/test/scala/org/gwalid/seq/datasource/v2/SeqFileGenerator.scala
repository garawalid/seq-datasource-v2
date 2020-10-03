package org.gwalid.seq.datasource.v2

import java.nio.charset.StandardCharsets

import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io._
import org.apache.hadoop.io.SequenceFile.Writer
import org.apache.hadoop.util.Progressable


class SeqFileGenerator {

  var keyClass: Class[_ <: Writable] = _
  var valueClass: Class[_ <: Writable] = _
  var keyData: Seq[Any] = _
  var valueData: Seq[Any] = _
  var r: Random = scala.util.Random
  // If the keyClass or valueClass is an ArrayWritable, we must set these two variables
  var keyClassArray: Class[_ <: Writable] = _
  var valueClassArray: Class[_ <: Writable] = _

  private def writeFile(path: Path): Unit = {

    if (keyData.length != valueData.length) {
      throw new Exception("keyData and valueData should have the same size.")
    }

    val optionFile = Writer.file(path)
    val optionBlockSize = Writer.blockSize(200)
    val optionBufferSize = Writer.replication(1.0.toShort)

    val reporter = new Reporter
    val optionReporter = Writer.progressable(reporter)

    val optionKeyClass = Writer.keyClass(keyClass)
    val optionValueClass = Writer.valueClass(valueClass)
    val conf = new Configuration


    val writer = SequenceFile.createWriter(conf, optionFile, optionBlockSize, optionBufferSize,
      optionReporter, optionKeyClass, optionValueClass)

    val key = WritableHelper.newInstance(keyClass, keyClassArray)
    val value = WritableHelper.newInstance(valueClass, valueClassArray)

    keyData.zip(valueData).foreach(x => {
      WritableHelper.setValue(key, x._1)
      WritableHelper.setValue(value, x._2)
      writer.append(key, value)

    })

    writer.hflush()
    writer.close()
  }


  def generateLongLong(filePath: Path): Unit = {
    keyData = for (i <- 0 until 100) yield r.nextLong()
    valueData = for (i <- 0 until 100) yield r.nextLong()
    keyClass = classOf[LongWritable]
    valueClass = classOf[LongWritable]
    writeFile(filePath)
  }


  def generateIntLong(filePath: Path): Unit = {
    keyData = for (i <- 0 until 100) yield r.nextInt()
    valueData = for (i <- 0 until 100) yield r.nextLong()
    keyClass = classOf[IntWritable]
    valueClass = classOf[LongWritable]
    writeFile(filePath)
  }

  def generateFloatBoolean(filePath: Path): Unit = {
    keyData = for (i <- 0 until 100) yield r.nextFloat()
    valueData = for (i <- 0 until 100) yield r.nextBoolean()
    keyClass = classOf[FloatWritable]
    valueClass = classOf[BooleanWritable]
    writeFile(filePath)
  }

  def generateDoubleInt(filePath: Path): Unit = {
    keyData = for (i <- 0 until 100) yield r.nextDouble
    valueData = for (i <- 0 until 100) yield r.nextInt
    keyClass = classOf[DoubleWritable]
    valueClass = classOf[IntWritable]
    writeFile(filePath)
  }

  def generateNullBytes(filePath: Path): Unit = {
    // Generate data (key:Null,value: Bytes)
    keyData = for (i <- 0 until 100) yield null
    valueData = for (i <- 0 until 100) yield generateRandomBytes()
    keyClass = classOf[NullWritable]
    valueClass = classOf[BytesWritable]
    writeFile(filePath)
  }

  def generateTextInt(filePath: Path): Unit = {
    keyData = for (_ <- 0 until 100) yield generateRandomString()
    valueData = for (_ <- 0 until 100) yield r.nextInt
    keyClass = classOf[Text]
    valueClass = classOf[IntWritable]
    writeFile(filePath)
  }

  def generateArrayOfIntInt(filePath: Path): Unit = {
    keyData = for (_ <- 0 until 100) yield Seq.fill(10)(r.nextInt())
    valueData = for (- <- 0 until 100) yield r.nextInt
    keyClass = classOf[ArrayWritable]
    valueClass = classOf[IntWritable]
    keyClassArray = classOf[IntWritable]
    writeFile(filePath)
  }

  def generateArrayOfTextInt(filePath: Path): Unit = {
    keyData = for (_ <- 0 until 100) yield Seq.fill(10)(generateRandomString())
    valueData = for (- <- 0 until 100) yield r.nextInt
    keyClass = classOf[ArrayWritable]
    valueClass = classOf[IntWritable]
    keyClassArray = classOf[Text]
    writeFile(filePath)
  }

  private def generateRandomString(): String = {
    val randomStr = new StringBuilder
    for (_ <- 0 to 10) yield randomStr.append(r.nextPrintableChar())
    randomStr.toString()
  }

  private def generateRandomBytes(): Seq[Byte] = {
    val randomString = generateRandomString()
    randomString.getBytes(StandardCharsets.UTF_8)
  }


  class Reporter extends Progressable {
    // Dummy object supposed to report stats.

    override def progress(): Unit = Unit

  }

  object WritableHelper {

    def setValue(writable: Writable, value: Any): Unit = {

      // NullWritable is a singleton and it's not working with match expression
      if (!writable.isInstanceOf[NullWritable]) {
        (writable, value) match {
          case (writable: LongWritable, value: Long) => writable.set(value)
          case (writable: DoubleWritable, value: Double) => writable.set(value)
          case (writable: FloatWritable, value: Float) => writable.set(value)
          case (writable: IntWritable, value: Int) => writable.set(value)
          case (writable: BooleanWritable, value: Boolean) => writable.set(value)
          case (writable: BytesWritable, value: Seq[Byte]) =>
            writable.set(value.toArray[Byte], 0, value.length)
          case (writable: Text, value: String) => writable.set(value)
          case (writable: ArrayWritable, value: Seq[Any]) =>
            val valueWritable: Array[Writable] = new Array[Writable](value.size)
              .map(_ => newInstance(writable.getValueClass.asSubclass(classOf[Writable])))

            valueWritable.zip(value).foreach(valueTuple => setValue(valueTuple._1, valueTuple._2))
            writable.set(valueWritable)
          case (writable: Any, value: Any) =>
            throw new RuntimeException(
              s"Unknown type of writable ${writable.getClass} and value ${value.getClass}"
            )
        }
      }
    }

    def newInstance(writable: Class[_ <: Writable],
                    arrayClassValue: Class[_ <: Writable]): Writable = {
      writable match {
        case writable if writable == classOf[NullWritable] => NullWritable.get()
        case writable if writable == classOf[ArrayWritable] =>
          new ArrayWritable(arrayClassValue)
        case writable => writable.newInstance()
      }
    }

    def newInstance(writable: Class[_ <: Writable]): Writable =
      newInstance(writable, classOf[NullWritable])

  }

}
