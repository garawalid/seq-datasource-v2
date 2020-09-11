package org.gwalid.SeqDataSourceV2

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.SequenceFile.Writer
import org.apache.hadoop.io._
import org.apache.hadoop.util.Progressable

import scala.util.Random

class SeqFileGenerator {

  var keyClass:Class[_<:Writable] = _ // Todo: Update the writeFile method
  var valueClass:Class[_ <:Writable] = _ // Todo: Update the writeFile method
  var keyData: Seq[Any] = _
  var valueData: Seq[Any] = _
  var r: Random = scala.util.Random

  def writeFile(keyClass: Class[_ <: Writable], valueClass: Class[_ <: Writable], keyData: Seq[Any], valueData: Seq[Any], path: Path): Unit = {

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


    val writer = SequenceFile.createWriter(conf, optionFile, optionBlockSize, optionBufferSize, optionReporter, optionKeyClass, optionValueClass)

    val key = keyClass.newInstance()
    val value = valueClass.newInstance()

    keyData.zip(valueData).foreach(x => {
      WritableHelper.setValue(key, x._1)
      WritableHelper.setValue(value, x._2)
      writer.append(key, value)

    })

    writer.hflush()
    writer.close()
  }


  def generateLongLong(filePath: Path): Unit = {

    keyData = for (i <- 0 to 100) yield r.nextLong()
    valueData = for (i <- 0 to 100) yield r.nextLong()
    writeFile(classOf[LongWritable], classOf[LongWritable], keyData, valueData, filePath)

  }


  def generateIntLong(filePath: Path): Unit = {

    keyData = for (i <- 0 to 100) yield r.nextInt()
    valueData = for (i <- 0 to 100) yield r.nextLong()
    writeFile(classOf[IntWritable], classOf[LongWritable], keyData, valueData, filePath)


  }

  def generateFloatBoolean(filePath: Path):Unit = {

    keyData = for (i <- 0 to 100) yield r.nextFloat()
    valueData = for (i <- 0 to 100) yield r.nextBoolean()
    writeFile(classOf[FloatWritable],classOf[BooleanWritable], keyData, valueData, filePath)

  }

  def generateDoubleInt(filePath: Path):Unit = {
    keyData = for( i <- 0 to 100) yield r.nextDouble
    valueData = for(i <- 0 to 100) yield r.nextInt
    writeFile(classOf[DoubleWritable], classOf[IntWritable],keyData, valueData,filePath)
  }

  class Reporter extends Progressable {
    /*
       Dummy object supposed to report stats.
        */ override def progress(): Unit = Unit


  }

  object WritableHelper {

    def setValue(writable: Writable, value: Any): Unit = {
      (writable, value) match {
        case (writable: LongWritable, value: Long) => writable.set(value)
        case (writable: DoubleWritable, value: Double) => writable.set(value)
        case (writable: FloatWritable, value: Float) => writable.set(value)
        case (writable: IntWritable, value: Int) => writable.set(value)
        case (writable: BooleanWritable, value: Boolean) => writable.set(value)
        case (_: NullWritable, _: Any) =>
        case (writable: ArrayWritable, _: Any) => throw new NotImplementedError(s"The ${writable.getClass.getName} is not supported yet!")
        // | writable: ByteWritable | writable: BytesWritable| writable: ShortWritable | writable: EnumSetWritable | writable: MapWritable | ObjectWritable
        case (writable: Any, value: Any) => throw new RuntimeException(s"Unknown type of writable ${writable.getClass} and value ${value.getClass}")

      }

    }

  }

}
