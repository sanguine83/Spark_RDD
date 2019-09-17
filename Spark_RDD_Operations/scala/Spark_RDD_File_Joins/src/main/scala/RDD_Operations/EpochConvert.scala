package RDD_Operations

import java.util.Date
import java.text.SimpleDateFormat

class EpochConvert(epochMillis: Long) {

  def epochToDate(epochMillis: Long): String = {
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy#MM#dd#hh")
    df.format(epochMillis*1000)
  }
}
