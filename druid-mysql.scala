import java.io.File
import java.io.PrintWriter
import java.text.SimpleDateFormat
import java.util.Date
import java.sql.Timestamp

// create output files
val pwMySql = new PrintWriter(new File("mysql.sql"))
val pwDruidSchema = new PrintWriter(new File("druid-schema.json"))
val pwDruidIndex = new PrintWriter(new File("druid-index.json"))

// create test database and table
pwMySql.write("drop database if exists `test`;\n")
pwMySql.write("create database if not exists `test`;\n")
pwMySql.write("use `test`;\n")
pwMySql.write("drop table if exists `test`;\n")

// table settings
val nRow = 100
val nCol = 5
val nRepeat = 10 // how many rows to use for repeating values

// create table
val mySqlSchema = new StringBuilder("create table `test` (`time484c39f77ace4808a5a48f6b481bafbe` TIMESTAMP")
val druidSchema = new StringBuilder("{\"type\" : \"index\", \"spec\" : {\"dataSchema\" : {\"dataSource\" : \"test\", \"parser\" : { \"type\" : \"string\", \"parseSpec\" : { \"format\" : \"json\", \"dimensionsSpec\" : { \"dimensions\" : [")
val metrics = new StringBuilder();
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
    + ", `dateInc" + i + "` DATE" // incremental date
  )
  druidSchema.append(if (i == 1) "" else ",")
  druidSchema.append(
    // "\"intInc" + i + "\"" // incremental int
    // + ", \"intRep" + i + "\"" // repeating int
    // + ", \"dblInc" + i + "\"" // incremental double
    // + ", \"dblRep" + i + "\"" // repeating double
    "\"strInc" + i + "\"" // incremental string
    + ", \"strRep" + i + "\"" // repeating string
    + ", \"decInc" + i + "\"" // incremental decimal
    + ", \"decRep" + i + "\"" // repeating decimal
    + ", \"boolInc" + i + "\"" // incremental bool
    + ", \"boolRep" + i + "\"" // repeating bool
    + ", \"smallInc" + i + "\"" // incremental small
    + ", \"smallRep" + i + "\"" // repeating small
    + ", \"dateCur" + i + "\"" // timestamp date
    + ", \"dateInc" + i + "\"" // incremental date
  )
  metrics.append(if (i == 1) "" else ",")
  metrics.append(
    "{\"type\":\"count\", \"name\": \"intInc" + i + "\", \"fieldName\": \"intInc" + i + "\"}" // incremental int
    + ", {\"type\":\"count\", \"name\":\"intRep" + i + "\", \"fieldName\":\"intRep" + i + "\"}" // repeating int
    + ", {\"type\":\"doubleSum\", \"name\":\"dblInc" + i + "\", \"fieldName\":\"dblInc" + i + "\"}" // incremental double
    + ", {\"type\":\"doubleSum\", \"name\":\"dblRep" + i + "\", \"fieldName\":\"dblRep" + i + "\"}" // repeating double
  )
}
mySqlSchema.append(");\n")
druidSchema.append("]},\"timestampSpec\": {\"column\": \"time484c39f77ace4808a5a48f6b481bafbe\", \"format\": \"iso\"}}},\"metricsSpec\" : [" + metrics.toString + "],\"granularitySpec\" : {\"type\" : \"uniform\",\"segmentGranularity\" : \"day\",\"queryGranularity\" : \"none\",\"intervals\" : [\"2019-01-01/2020-01-01\"],\"rollup\" : false}},\"ioConfig\" : {\"type\" : \"index\",\"firehose\" : {\"type\" : \"local\",\"baseDir\" : \"quickstart/tutorial/\",\"filter\" : \"test.json\"},\"appendToExisting\" : false},\"tuningConfig\" : {\"type\" : \"index\",\"maxRowsPerSegment\" : 5000000,\"maxRowsInMemory\" : 25000,\"forceExtendableShardSpecs\" : true}}}")
pwMySql.write(mySqlSchema.toString)
pwDruidSchema.write(druidSchema.toString)
pwDruidSchema.close

// add rows
val ts = Timestamp.valueOf("2019-01-01 00:00:00")
val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
val timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
val timestampDruidFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
for (i <- 1 to nRow) {
  val timestampLong = ts.getTime + i * 1000L
  val timestamp = new Timestamp(timestampLong)
  val timestampMySqlString = timestampFormat.format(timestamp)
  val timestampDruidString = timestampDruidFormat.format(timestamp)
  val mySqlQuery = new StringBuilder("insert into `test` values ('" + timestampMySqlString + "'")
  val druidQuery = new StringBuilder("{\"time484c39f77ace4808a5a48f6b481bafbe\":\"" + timestampDruidString + "\"")
  for (j <- 1 to nCol) {
    mySqlQuery.append(
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
    druidQuery.append(
      ", \"intInc" + j + "\": " + "\"" + i + "\"" // incremental int
      + ", \"intRep" + j + "\": " + "\"" + (i % nRepeat).toInt + "\"" // repeating int
      + ", \"dblInc" + j + "\": " + "\"" + (i / 100d) + "\"" // incremental double
      + ", \"dblRep" + j + "\": " + "\"" + (i % nRepeat / 100d) + "\"" // repeating double
      + ", \"strInc" + j + "\": " + "\"strstrstrstrstrstr" + i + "\"" // incremental string
      + ", \"strRep" + j + "\": " + "\"strstrstrstrstrstr" + (i % nRepeat).toLong + "\"" // repeating string
      + ", \"decInc" + j + "\": " + "\"" + (i / 100d) + "\"" // incremental decimal
      + ", \"decRep" + j + "\": " + "\"" + (i % nRepeat / 100d) + "\"" // repeating decimal
      + ", \"boolInc" + j + "\": " + "\"" + (i % 2) + "\"" // incremental bool
      + ", \"boolRep" + j + "\": " + "\"" + ((i / nRepeat).toLong % 2) + "\"" // repeating bool
      + ", \"smallInc" + j + "\": " + "\"" + (i % 32768).toShort + "\"" // incremental short
      + ", \"smallRep" + j + "\": " + "\"" + (i % 32768 % nRepeat).toShort + "\"" // repeating short
      + ", \"dateCur" + j + "\": " + "\"" + dateFormat.format(timestampLong) + "\"" // timestamp date
      + ", \"dateInc" + j + "\": " + "\"" + dateFormat.format(new Date(timestampLong + i * 86400L * 1000L)) + "\"" // incremental date
    )
  }
  mySqlQuery.append(");\n")
  pwMySql.write(mySqlQuery.toString)
  druidQuery.append("}\n")
  pwDruidIndex.write(druidQuery.toString)
}

pwMySql.close
pwDruidIndex.close

