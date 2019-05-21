import java.io.File
import java.io.PrintWriter
import java.text.SimpleDateFormat
import java.util.Date
import java.sql.Timestamp

// create output SQL script file
val pw = new PrintWriter(new File("mysql.sql"))

// create test database and table
pw.write("drop database if exists `test`;\n")
pw.write("create database if not exists `test`;\n")
pw.write("use `test`;\n")
pw.write("drop table if exists `test`;\n")

// table settings
val nRow = 100000
val nCol = 5
val nRepeat = 10 // how many rows to use for repeating values

// create table
val createTableQuery = new StringBuilder("create table `test` (`timestamp` TIMESTAMP")
for (i <- 1 to nCol) {
  createTableQuery.append(
    ", `intInc" + i + "` INTEGER" // incremental int
    + ", `intRep" + i + "` INTEGER" // repeating int
    + ", `dblInc" + i + "` DOUBLE" // incremental double
    + ", `dblRep" + i + "` DOUBLE" // repeating double
    + ", `strInc" + i + "` VARCHAR(30)" // incremental string
    + ", `strRep" + i + "` VARCHAR(30)" // repeating string
    + ", `decInc" + i + "` DECIMAL(10,4)" // incremental decimal
    + ", `decRep" + i + "` DECIMAL(10,4)" // repeating decimal
    + ", `boolInc" + i + "` BOOLEAN" // incremental bool
    + ", `boolRep" + i + "` BOOLEAN" // repeating bool
    + ", `smallInc" + i + "` SMALLINT" // incremental small
    + ", `smallRep" + i + "` SMALLINT" // repeating small
    + ", `dateCur" + i + "` DATE" // timestamp date
    + ", `dateInc" + i + "` DATE" // incremental date
  )
}
createTableQuery.append(");\n")
pw.write(createTableQuery.toString)

// add rows
val ts = Timestamp.valueOf("2019-01-01 00:00:00")
val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
val timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
for (i <- 1 to nRow) {
  val timestampLong = ts.getTime() + i * 1000L
  val timestamp = new Timestamp(timestampLong)
  val timestampString = timestampFormat.format(timestamp)
  val insertQuery = new StringBuilder("insert into `test` values ('" + timestampString + "'")
  for (j <- 1 to nCol) {
    insertQuery.append(
      ", " + i // incremental int
      + ", " + (i % nRepeat).toInt // repeating int
      + ", " + (i / 100d) // incremental double
      + ", " + (i % nRepeat / 100d) // repeating double
      + ", 'strstrstrstrstrstr" + i + "'" // incremental string
      + ", 'strstrstrstrstrstr" + (i % nRepeat).toLong + "'" // repeating string
      + ", " + (i / 100d) // incremental decimal
      + ", " + (i % nRepeat / 100d) // repeating decimal
      + ", " + (i % 2) // incremental bool
      + ", " + ((i / nRepeat).toLong % 2) // repeating bool
      + ", " + (i % 32768).toShort // incremental short
      + ", " + (i % 32768 % nRepeat).toShort // repeating short
      + ", '" + dateFormat.format(timestampLong) + "'" // timestamp date
      + ", '" + dateFormat.format(new Date(timestampLong + i * 86400L * 1000L)) + "'" // incremental date
    )
  }
  insertQuery.append(");\n")
  pw.write(insertQuery.toString)
}

pw.close()

