package org.gwalid.SeqDataSourceV2

class SeqFileIO {

  def newInputFile(path:String):SeqInputFileIO={
    new SeqInputFileIO(path)
  }


}
