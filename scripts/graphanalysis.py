from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc

# ── Initialise Spark ──────────────────────────────────────────
spark = SparkSession.builder \
    .appName("NYC Rideshare Graph Analysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ── Load enriched data ────────────────────────────────────────
joined = spark.read.csv("output_files/joined_df.csv",
                        header=True, inferSchema=True)

# ── Task 6: Route corridors ───────────────────────────────────
corridors = joined.groupBy("pickup_zone", "dropoff_zone") \
    .agg(count("*").alias("trip_count")) \
    .orderBy(desc("trip_count"))

corridors.show(15)
corridors.toPandas().to_csv("output_files/route_corridors.csv", index=False)

# ── Task 7: Graph model — vertices and edges ──────────────────
# Vertices = unique zones
vertices = joined.selectExpr("pickup_zone as id").union(
    joined.selectExpr("dropoff_zone as id")
).distinct()

# Edges = trip counts between zones (weighted)
edges = joined.groupBy(
    col("pickup_zone").alias("src"),
    col("dropoff_zone").alias("dst")
).agg(count("*").alias("weight")) \
 .orderBy(desc("weight"))

print(f"Vertices (zones): {vertices.count()}")
print(f"Edges (routes):   {edges.count()}")
edges.show(10)

edges.toPandas().to_csv("output_files/graph_edges.csv", index=False)
vertices.toPandas().to_csv("output_files/graph_vertices.csv", index=False)

print("✓ Graph analysis complete.")
spark.stop()