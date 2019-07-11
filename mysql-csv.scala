import java.io.File
import java.io.PrintWriter
import java.text.SimpleDateFormat
import java.util.Date
import java.sql.Timestamp

// create output files
val pwMySql = new PrintWriter(new File("mysql.sql"))
val pwCsv = new PrintWriter(new File("bigdata.csv"))

// create test database and table
pwMySql.write("drop database if exists `bigdata_db`;\n")
pwMySql.write("create database if not exists `bigdata_db`;\n")
pwMySql.write("use `bigdata_db`;\n")
pwMySql.write("drop table if exists `bigdata_tbl`;\n")

// table settings
val nRow = 10000000
val nCol = 5
val nRepeat = 10 // how many rows to use for repeating values

// create table
val mySqlSchema = new StringBuilder("create table `bigdata_tbl` (`time484c39f77ace4808a5a48f6b481bafbe` TIMESTAMP")
val csvSchema = new StringBuilder("time484c39f77ace4808a5a48f6b481bafbe")
for (i <- 1 to nCol) {
  mySqlSchema.append(
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
  )
  csvSchema.append(
    ",intInc" + i // incremental int
    + ",intRep" + i // repeating int
    + ",dblInc" + i // incremental double
    + ",dblRep" + i // repeating double
    + ",strInc" + i // incremental string
    + ",strRep" + i // repeating string
    + ",decInc" + i // incremental decimal
    + ",decRep" + i // repeating decimal
    + ",boolInc" + i // incremental bool
    + ",boolRep" + i // repeating bool
    + ",smallInc" + i // incremental small
    + ",smallRep" + i // repeating small
    + ",dateCur" + i // timestamp date
  )
}
mySqlSchema.append(");\n")
csvSchema.append("\n")
pwMySql.write(mySqlSchema.toString)
pwCsv.write(csvSchema.toString)

// add rows
val ts = Timestamp.valueOf("2019-01-01 00:00:00")
val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
val timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
val timestampDruidFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
for (i <- 1 to nRow) {
  val timestampLong = ts.getTime + i * 1000L
  val timestamp = new Timestamp(timestampLong)
  val timestampMySqlString = timestampFormat.format(timestamp)
  //val mySqlQuery = new StringBuilder("insert into `bigdata_tbl` values ('" + timestampMySqlString + "'")
  val csvQuery = new StringBuilder(timestampMySqlString)
  for (j <- 1 to nCol) {
    // mySqlQuery.append(
    //   ", " + i // incremental int
    //   + ", " + (i % nRepeat).toInt // repeating int
    //   + ", " + (i / 100d) // incremental double
    //   + ", " + (i % nRepeat / 100d) // repeating double
    //   + ", 'strstrstrstrstrstr" + i + "'" // incremental string
    //   + ", 'strstrstrstrstrstr" + (i % nRepeat).toLong + "'" // repeating string
    //   + ", " + (i / 100d) // incremental decimal
    //   + ", " + (i % nRepeat / 100d) // repeating decimal
    //   + ", " + (i % 2) // incremental bool
    //   + ", " + ((i / nRepeat).toLong % 2) // repeating bool
    //   + ", " + (i % 32768).toShort // incremental short
    //   + ", " + (i % 32768 % nRepeat).toShort // repeating short
    //   + ", '" + dateFormat.format(timestampLong) + "'" // timestamp date
    //   + ", '" + dateFormat.format(new Date(timestampLong + i * 86400L * 1000L)) + "'" // incremental date
    // )
    csvQuery.append(
      "," + i // incremental int
      + "," + (i % nRepeat).toInt // repeating int
      + "," + (i / 100d) // incremental double
      + "," + (i % nRepeat / 100d) // repeating double
      + ",'strstrstrstrstrstr" + i + "'" // incremental string
      + ",'strstrstrstrstrstr" + (i % nRepeat).toLong + "'" // repeating string
      + "," + (i / 100d) // incremental decimal
      + "," + (i % nRepeat / 100d) // repeating decimal
      + "," + (i % 2) // incremental bool
      + "," + ((i / nRepeat).toLong % 2) // repeating bool
      + "," + (i % 32768).toShort // incremental short
      + "," + (i % 32768 % nRepeat).toShort // repeating short
      + "," + dateFormat.format(timestampLong) + "" // timestamp date
    )
  }
  //mySqlQuery.append(");\n")
  //pwMySql.write(mySqlQuery.toString)
  csvQuery.append("\n")
  pwCsv.write(csvQuery.toString)
}

pwMySql.write("LOAD DATA INFILE '/var/lib/mysql-files/bigdata.csv' INTO TABLE bigdata_tbl FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\n' IGNORE 1 ROWS;")

pwMySql.close
pwCsv.close

