import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{BooleanType, DateType, TimestampType}

object SimpleApp {
  def main(args: Array[String]): Unit = {
    // Initialisation de Spark
    val spark = SparkSession.builder.appName("ETL").master("local[4]").getOrCreate()
    // Lecture du fichier business.json
      val businessFile = "data/yelp_academic_dataset_business.json"
      //	Dataframe Business (business_id, business_name, category)
      var fact_business_info = spark.read.json(businessFile).cache()
      fact_business_info = fact_business_info
        .withColumn("categories", explode(org.apache.spark.sql.functions.split(col("categories"), ",")))
      fact_business_info = fact_business_info
        .withColumnRenamed("categories", "category_name")
      fact_business_info = fact_business_info
        .filter(col("category_name").notEqual("None"))
      fact_business_info = fact_business_info
        .drop(col("categories"))

      // Dataframe Business (business_id, name, category, is_open)
      var business = fact_business_info.select("business_id","name","category_name","is_open")

      // Dataframe Parking (parking_id, BikeParking, Garage, Street, Validated, lot)
      var parking = fact_business_info.select("business_id", "attributes.BusinessParking", "attributes.BikeParking")
      //Supression des doublons
      parking = parking.dropDuplicates()


      
      //Identifiant parking_id, id qui va incrémenter automatiquement
      parking = parking.withColumn("parking_id", monotonically_increasing_id())

      //On enlève les mots précédants "True" et "False"
      //Séparer les différents attributs du parking
      parking = parking
        .withColumn("Garage", split(col("BusinessParking"), ",").getItem(0))
        .withColumn("Street", split(col("BusinessParking"), ",").getItem(1))
        .withColumn("Validated", split(col("BusinessParking"), ",").getItem(2))
        .withColumn("lot", split(col("BusinessParking"), ",").getItem(3))
        .drop("BusinessParking")
      //Garder que les mots Garage, Street, Validated, lot
      parking = parking
        .withColumn("Garage", substring(col("Garage"), 12, 99))
        .withColumn("Street", substring(col("Street"), 12, 99))
        .withColumn("Validated", substring(col("Validated"), 15, 99))
        .withColumn("lot", substring(col("lot"), 9, 99))
      //Remplacement des valeurs vide par "false"
      parking = parking
        .withColumn("Garage", col("Garage").cast(BooleanType))
        .withColumn("Street", col("Street").cast(BooleanType))
        .withColumn("Validated", col("Validated").cast(BooleanType))
        .withColumn("lot", col("lot").cast(BooleanType))
        .withColumn("BikeParking", col("BikeParking").cast(BooleanType))
      parking = parking.na.fill(false)

      //Business_id sera une clé étrangère que l'on va supprimer plus tard lors de la jointure
      parking = parking
        .withColumnRenamed("business_id", "fk_business_id")
      parking = parking
        .select("parking_id", "BikeParking", "Garage", "Street", "Validated", "lot", "fk_business_id")

      //Dataframe Hours (hours_id, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday)
      var hours = fact_business_info
        .select("business_id",
        "hours.Monday",
        "hours.Tuesday",
        "hours.Wednesday",
        "hours.Thursday",
        "hours.Friday",
        "hours.Saturday",
        "hours.Sunday")
      //Supression des doublons
      hours = hours.dropDuplicates()
      //Identifiant parking_id, id qui va incrémenter automatiquement
      hours = hours
        .withColumn("hours_id", monotonically_increasing_id())
      hours = hours
        .withColumnRenamed("business_id", "fk_business_id")

      //Dataframe Location (location_id, address, city, state, postal_code, longitude, latitude)
      var location = fact_business_info
        .select("business_id",
        "address",
        "city",
        "state",
        "postal_code",
        "longitude",
        "latitude")
      //Supression des doublons
      location = location.dropDuplicates()
      //Identifiant parking_id, id qui va incrémenter automatiquement
      location = location
        .withColumn("location_id", monotonically_increasing_id())

    parking.printSchema()

    // Lecture du fichier tip.csv
    var tip = spark.read
      .option("header",true)
      .csv("data/yelp_academic_dataset_tip.csv")
      .select("business_id","compliment_count","date","text")
    // Auto incrémentation de l'identifiant tip_id
    tip = tip
      .withColumn("tip_id", monotonically_increasing_id())
    tip = tip
      .withColumnRenamed("business_id", "fk_business_id")
    // Cast de la colonne date en timestamp
    tip = tip
      .withColumn("date", col("date").cast(TimestampType))
    // Dataframe Tip (tip_id, text, date, compliment_count, business_id)
    tip = tip
      .select("tip_id","text","date","compliment_count","fk_business_id")
    // Affichage du dataframe Tip
    tip.printSchema()


    // Lecture du fichier checkin.json
    var checkin = spark.read
      .json("data/yelp_academic_dataset_checkin.json")
    //checkin = checkin
     // .withColumn("business_id", substring(col("business_id"), 3, 99))
    // Auto incrémentation de l'identifiant checkin_id
    checkin = checkin
      .withColumn("checkin_id", monotonically_increasing_id())
    // On éclate le champ date, on compte le nombre de virgules car entre chaque virgule il y a une date
    checkin = checkin
      .withColumn("total_checkin", size(split(col("date"), ",")))

    checkin = checkin
      .withColumnRenamed("business_id", "fk_business_id")

    // Affichage du dataframe Checkin
    checkin.printSchema()

    

    /*
    //Jointure pour faire la table de fait "Information Business"
    fact_business_info = fact_business_info.join(parking, fact_business_info("business_id") === parking("fk_business_id"), "inner")
    parking = parking.drop("fk_business_id")
    fact_business_info = fact_business_info.drop("fk_business_id")
    */


    /*	Table temporaire
    fact_business_info.createOrReplaceTempView("BUS")
    parking.createOrReplaceTempView("PARK")
    */

    //fact_business_info.printSchema()
    //fact_business_info.show
    //parking.printSchema()
    //parking.show
    //hours.printSchema()
    //hours.show
    //location.printSchema()
    //location.show



    // Paramètres de la connexion BD
    import java.util.Properties
    // BD ORACLE
    Class.forName("oracle.jdbc.driver.OracleDriver")
    val urlOracle = "jdbc:oracle:thin:@stendhal:1521:enss2022"
    val connectionPropertiesOracle = new Properties()
    connectionPropertiesOracle.setProperty("driver", "oracle.jdbc.driver.OracleDriver")
    connectionPropertiesOracle.setProperty("user", "ct434953")
    connectionPropertiesOracle.setProperty("password", "ct434953")


    // BD POSTGRESQL
    val urlPostgreSQL = "jdbc:postgresql://stendhal:5432/tpid2020"
    val connectionProporetiesPostgreSQL = new Properties()
    connectionProporetiesPostgreSQL.setProperty("driver", "org.postgresql.Driver")
    connectionProporetiesPostgreSQL.put("user", "tpid")
    connectionProporetiesPostgreSQL.put("password", "tpid")

    // Lecture et affichage de la table user de PostgreSQL
    var users = spark.read
      .option("numPartitions", 10)
      .option("partitionColumn", "yelping_since")
      .option("lowerBound", "2010-01-01")
      .option("upperBound", "2019-12-31")
      .jdbc(urlPostgreSQL, "yelp.user", connectionProporetiesPostgreSQL)
    //users.show(2)

    // Lecture et affichage de la table review de PostgreSQL
    // Dataframe Review (review_id, business_id, user_id, date, text, funny, stars)
    var review = spark.read
      .format("jdbc")
      .option("url",urlPostgreSQL)
      .option("user", "tpid")
      .option("password", "tpid")
      .option("dbtable","(select review_id,business_id,user_id,date,text,funny,stars from yelp.review) as subq")
      .option("partitionColumn", "date")
      .option("lowerBound", "2010-01-01")
      .option("upperBound", "2019-12-31")
      .option("numPartitions", 40)
      .load()

    review = review
      .withColumnRenamed("business_id", "fk_business_id")
    


    // ATTENTION PROBLÈME DE MÉMOIRE
    // Lecture et affichage de la table friend de PostgreSQL
    var friend = spark.read
      .option("dbtable", "(select user_id, friend_id from yelp.friend) as subquery")
      .option("user", "tpid")
      .option("password", "tpid")
      .jdbc(urlPostgreSQL, "yelp.friend", connectionProporetiesPostgreSQL)
    //friend.show(2)
  
    // Lecture et affichage de la table elite de PostgreSQL
    var elite = spark.read
      .option("user", "tpid")
      .option("password", "tpid")
      .jdbc(urlPostgreSQL, "yelp.elite", connectionProporetiesPostgreSQL)
    //elite.show(2)


    fact_business_info.show(2)
    review.show(2)
    checkin.show(2)
    tip.show(2)
    parking.show(2)
    hours.show(2)


    fact_business_info.createOrReplaceTempView("FACT")
    review.createOrReplaceTempView("REVIEW")
    checkin.createOrReplaceTempView("CHECKIN")
    tip.createOrReplaceTempView("TIP")
    parking.createOrReplaceTempView("PARK")
    hours.createOrReplaceTempView("HOURS")
    business.createOrReplaceTempView("BUSINESS")

    var fBusiness = 
      spark
      .sql("SELECT * FROM PARK p, HOURS h, CHECKIN c, TIP t, REVIEW r, BUSINESS b WHERE b.business_id = h.fk_business_id AND b.business_id = c.fk_business_id AND b.business_id = t.fk_business_id AND b.business_id = r.fk_business_id AND b.business_id = p.fk_business_id")
    fBusiness = fBusiness.select("business_id","hours_id","checkin_id","tip_id","review_id","parking_id")
    fBusiness.show()

    
		/*
    // Enregistrement du DataFrame users dans la table "user"
    users.write
    	.mode(SaveMode.Overwrite).jdbc(urlOracle, "yelp.\"user\"", connectionPropertiesOracle)

    // Enregistrement du DataFrame friends dans la table "friend"
    fact_business_info.write
      .mode(SaveMode.Overwrite)
      .jdbc(urlOracle, "fact_business_info", connectionPropertiesOracle)

    categories.write
      .mode(SaveMode.Overwrite)
      .jdbc(urlOracle, "categories", connectionPropertiesOracle)
    */


    import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
    val dialect = new OracleDialect
    JdbcDialects.registerDialect(dialect)
    spark.stop()
  }
  import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
  import org.apache.spark.sql.types._
  class OracleDialect extends JdbcDialect {
    override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
      case BooleanType => Some(JdbcType("NUMBER(1)", java.sql.Types.INTEGER))
      case IntegerType => Some(JdbcType("NUMBER(10)", java.sql.Types.INTEGER))
      case LongType => Some(JdbcType("NUMBER(19)", java.sql.Types.BIGINT))
      case FloatType => Some(JdbcType("NUMBER(19, 4)", java.sql.Types.FLOAT))
      case DoubleType => Some(JdbcType("NUMBER(19, 4)", java.sql.Types.DOUBLE))
      case ByteType => Some(JdbcType("NUMBER(3)", java.sql.Types.SMALLINT))
      case ShortType => Some(JdbcType("NUMBER(5)", java.sql.Types.SMALLINT))
      case StringType => Some(JdbcType("VARCHAR2(4000)", java.sql.Types.VARCHAR))
      case DateType => Some(JdbcType("DATE", java.sql.Types.DATE))
      //Ligne ajoutée pour timestamp
      case TimestampType => Some(JdbcType("TIMESTAMP",java.sql.Types.TIMESTAMP))
      case _ => None
    }
    override def canHandle(url: String): Boolean = url.startsWith("jdbc:oracle")
  }
}