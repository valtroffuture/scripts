import java.io._
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
pw.write("create table `test` (`timestamp` TIMESTAMP"
  + ", `int1` INTEGER, `dbl1` DOUBLE, `str1` VARCHAR(30), `dec1` DECIMAL(10,4), `bool1` BOOLEAN, `small1` SMALLINT, `date1` DATE"
  + ", `int2` INTEGER, `dbl2` DOUBLE, `str2` VARCHAR(30), `dec2` DECIMAL(10,4), `bool2` BOOLEAN, `small2` SMALLINT, `date2` DATE" 
  + ", `int3` INTEGER, `dbl3` DOUBLE, `str3` VARCHAR(60), `dec3` DECIMAL(10,4), `bool3` BOOLEAN, `small3` SMALLINT, `date3` DATE" 
  + ", `int4` INTEGER, `dbl4` DOUBLE, `str4` VARCHAR(60), `dec4` DECIMAL(10,4), `bool4` BOOLEAN, `small4` SMALLINT, `date4` DATE" 
  + ", `int5` INTEGER, `dbl5` DOUBLE, `str5` VARCHAR(30), `dec5` DECIMAL(10,4), `bool5` BOOLEAN, `small5` SMALLINT, `date5` DATE"
  + ", `int6` INTEGER, `dbl6` DOUBLE, `str6` VARCHAR(30), `dec6` DECIMAL(10,4), `bool6` BOOLEAN, `small6` SMALLINT, `date6` DATE"
  + ", `int7` INTEGER, `dbl7` DOUBLE, `str7` VARCHAR(30), `dec7` DECIMAL(10,4), `bool7` BOOLEAN, `small7` SMALLINT, `date7` DATE"
  + ", `int8` INTEGER, `dbl8` DOUBLE, `str8` VARCHAR(30), `dec8` DECIMAL(10,4), `bool8` BOOLEAN, `small8` SMALLINT, `date8` DATE"
  + ", `int9` INTEGER, `dbl9` DOUBLE, `str9` VARCHAR(30), `dec9` DECIMAL(10,4), `bool9` BOOLEAN, `small9` SMALLINT, `date9` DATE"
  + ", `int10` INTEGER, `dbl10` DOUBLE, `str10` VARCHAR(30), `dec10` DECIMAL(10,4), `bool10` BOOLEAN, `small10` SMALLINT, `date10` DATE"
  + ");\n");

// add rows
val ts = Timestamp.valueOf("2019-01-01 00:00:00")
val n = 100000
val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
val timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
for(i <- 1 to n) {
  val timestampLong = ts.getTime() + i * 1000L
  val timestamp = new Timestamp(timestampLong)
  val timestampString = timestampFormat.format(timestamp)

  pw.write("insert into `test` values ('" 
    + timestampString + "', " 

    + i + ", " 
    + (i / 100d) 
    + ", 'strstrstrstrstrstr" + i 
    + "', " + (i / 10000d) 
    + ", " + (i % 2) 
    + ", " + (i % 32768) 
    + ", '" + dateFormat.format(new Date(timestampLong + i * 86400L * 1000L))

    + "', " + (i % 10)
    + ", " + (i % 10 / 100d) 
    + ", 'strstrstrstrstrstr" + (i % 10)
    + "', " + (i % 10 / 10000d) 
    + ", " + 1 
    + ", " + (i % 32768 % 10)
    + ", '" + dateFormat.format(new Date(timestampLong))

    + "', " + (i % 100)
    + ", " + (i % 100 / 100d) 
    + ", 'strstrstrstrstrstrstrstrstrstrstrstr" + i
    + "', " + (i % 100 / 10000d) 
    + ", " + 0 
    + ", " + (i % 32768 % 100)
    + ", '" + dateFormat.format(new Date(timestampLong + (i % 10) * 86400L * 1000L))

    + "', " + (i % 1000)
    + ", " + (i % 1000 / 100d) 
    + ", 'strstrstrstrstrstrstrstrstrstrstrstr" + (i % 10)
    + "', " + (i % 1000 / 10000d) 
    + ", " + ((i / 10).toInt % 2)
    + ", " + (i % 32768 % 1000)
    + ", '" + dateFormat.format(new Date(timestampLong + (i % 100) * 86400L * 1000L))

    + "', " + (i % 10000)
    + ", " + (i % 10000 / 100d) 
    + ", 'strstrstrstrstrstr" + (i % 100)
    + "', " + (i % 10000 / 10000d) 
    + ", " + ((i / 100).toInt % 2)
    + ", " + (i % 32768 % 10000)
    + ", '" + dateFormat.format(new Date(timestampLong + (i % 1000) * 86400L * 1000L))

    + "', " + (i % 100000)
    + ", " + (i % 100000 / 100d) 
    + ", 'strstrstrstrstrstr" + (i % 1000)
    + "', " + (i % 100000 / 10000d) 
    + ", " + ((i / 1000).toInt % 2)
    + ", " + (i % 32768 % 100000)
    + ", '" + dateFormat.format(new Date(timestampLong + (i % 10000) * 86400L * 1000L))

    + "', " + (i % 1000000)
    + ", " + (i % 1000000 / 100d) 
    + ", 'strstrstrstrstrstr" + (i % 10000)
    + "', " + (i % 1000000 / 10000d) 
    + ", " + ((i / 10000).toInt % 2)
    + ", " + (i % 32768 % 1000000)
    + ", '" + dateFormat.format(new Date(timestampLong + (i % 100000) * 86400L * 1000L))

    + "', " + (i % 10000000)
    + ", " + (i % 10000000 / 100d) 
    + ", 'strstrstrstrstrstr" + (i % 100000)
    + "', " + (i % 10000000 / 10000d) 
    + ", " + ((i / 100000).toInt % 2)
    + ", " + (i % 32768 % 10000000)
    + ", '" + dateFormat.format(new Date(timestampLong + (i % 100000) * 86400L * 1000L))

    + "', " + (i % 100000000)
    + ", " + (i % 100000000 / 100d) 
    + ", 'strstrstrstrstrstr" + (i % 1000000)
    + "', " + (i % 100000000 / 10000d) 
    + ", " + ((i / 1000000).toInt % 2)
    + ", " + (i % 32768 % 100000000)
    + ", '" + dateFormat.format(new Date(timestampLong + (i % 1000000) * 86400L * 1000L))

    + "', " + (i % 1000000000)
    + ", " + (i % 1000000000 / 100d) 
    + ", 'strstrstrstrstrstr" + (i % 10000000)
    + "', " + (i % 1000000000 / 10000d) 
    + ", " + ((i / 10000000).toInt % 2)
    + ", " + (i % 32768 % 1000000000)
    + ", '" + dateFormat.format(new Date(timestampLong + (i % 10000000) * 86400L * 1000L))

    + "');\n"
  )
}

pw.close()

