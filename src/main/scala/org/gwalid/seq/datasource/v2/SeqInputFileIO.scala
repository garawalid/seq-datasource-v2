package org.gwalid.seq.datasource.v2

import org.apache.hadoop.fs.Path

class SeqInputFileIO(path: String) extends Serializable {
  def getPath(): Path = new Path(path)
}
