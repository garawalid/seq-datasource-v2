package org.gwalid.SeqDataSourceV2

import org.apache.hadoop.fs.Path

class SeqInputFileIO(path:String) extends Serializable {
    def getPath():Path = new Path(path)
}
