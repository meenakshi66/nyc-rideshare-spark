from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, month, desc

# ── Initialise Spark ──────────────────────────────────────────
spark = SparkSession.builder \
    .appName("NYC Rideshare Analysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ── Load data ─────────────────────────────────────────────────
rideshare = spark.read.csv("data/rideshare_data.csv",
                           header=True, inferSchema=True)
zones = spark.read.csv("data/taxi_zone_lookup.csv",
                       header=True, inferSchema=True)

print("Rideshare rows:", rideshare.count())
print("Schema:")
rideshare.printSchema()

# ── Task 1: Join with zone lookup ─────────────────────────────
# Enrich pickup zones
joined = rideshare \
    .join(zones.withColumnRenamed("LocationID", "PULocationID")
               .withColumnRenamed("Zone", "pickup_zone")
               .withColumnRenamed("Borough", "pickup_borough"),
          on="PULocationID", how="left") \
    .join(zones.withColumnRenamed("LocationID", "DOLocationID")
               .withColumnRenamed("Zone", "dropoff_zone")
               .withColumnRenamed("Borough", "dropoff_borough"),
          on="DOLocationID", how="left")

joined.show(5)
joined.toPandas().to_csv("output_files/joined_df.csv", index=False)

# ── Task 2: Trip counts, driver earnings, platform profit ─────
monthly = joined.groupBy("business", month("pickup_datetime").alias("month")) \
    .agg(
        count("*").alias("trip_count"),
        sum("driver_total_pay").alias("driver_earnings"),
        sum("rideshare_profit").alias("platform_profit")
    ).orderBy("business", "month")

monthly.show()
monthly.toPandas().to_csv("output_files/monthly_summary.csv", index=False)

# ── Task 3: Most profitable routes ───────────────────────────
routes = joined.groupBy("pickup_borough", "dropoff_borough") \
    .agg(
        count("*").alias("trip_count"),
        sum("rideshare_profit").alias("total_profit")
    ).orderBy(desc("total_profit"))

routes.show(10)
routes.toPandas().to_csv("output_files/profitable_routes.csv", index=False)

# ── Task 4: Top pickup and dropoff zones ──────────────────────
top_pickups = joined.groupBy("pickup_zone") \
    .count().orderBy(desc("count")).limit(10)

top_dropoffs = joined.groupBy("dropoff_zone") \
    .count().orderBy(desc("count")).limit(10)

top_pickups.show()
top_dropoffs.show()
top_pickups.toPandas().to_csv("output_files/top_pickups.csv", index=False)
top_dropoffs.toPandas().to_csv("output_files/top_dropoffs.csv", index=False)

# ── Task 5: Avg waiting time in January ───────────────────────
jan = joined.filter(month("pickup_datetime") == 1)

avg_wait = jan.groupBy("business") \
    .agg(avg("wait_time_seconds").alias("avg_wait_seconds")) \
    .withColumn("avg_wait_minutes", col("avg_wait_seconds") / 60)

avg_wait.show()
avg_wait.toPandas().to_csv("output_files/avg_waiting_time.csv", index=False)

print("✓ All tasks complete. Output saved to output_files/")
spark.stop()