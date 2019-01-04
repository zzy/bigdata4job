package bd

import java.util.{ Date, Calendar }
import java.text.SimpleDateFormat

class Utils extends Serializable {

  def getCurMonth: String = {
    var today: Date = new Date()
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM")
    dateFormat.format(today)
  }
  
  def getToday: String = {
    var today: Date = new Date()
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    dateFormat.format(today)
  }

  def getYesterday: String = {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var calendar: Calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -1)
    dateFormat.format(calendar.getTime())
  }

  def batchNumber: String = {
    var today: Date = new Date()

    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
//    var batchNumber = 
    dateFormat.format(today)

//    if (!isAfternoon) {
//      batchNumber + "01"
//    }
//    else {
//      batchNumber + "02"
//    }
  }

  def isAfternoon: Boolean = {
    var today: Date = new Date()

    var hourFormat: SimpleDateFormat = new SimpleDateFormat("HH")
    var currentHour = hourFormat.format(today).toInt

    currentHour > 12
  }

}


