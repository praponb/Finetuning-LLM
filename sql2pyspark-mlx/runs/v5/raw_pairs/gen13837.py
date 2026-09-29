from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("sqft", "id")

metrics = (
    df_stores
    .withColumn("running_min_sqft", F.min("sqft").over(w))
    .withColumn("running_sum_sqft", F.sum("sqft").over(w))
    .withColumn("running_max_sqft", F.max("sqft").over(w))
    .withColumn("prev_sqft", F.lag("sqft", 2).over(w2))
)

result = (
    metrics
    .cube(
        F.when(F.col("running_min_sqft") >= 15400, "LARGE").otherwise("SMALL").alias("running_min_sqft_tier"),
        F.when(F.col("prev_sqft") >= 14447, "HIGH").otherwise("LOW").alias("prev_sqft_tier"),
    )
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_rows"),
    )
    .filter(F.col("tokyo_rows") < 1000)
)
