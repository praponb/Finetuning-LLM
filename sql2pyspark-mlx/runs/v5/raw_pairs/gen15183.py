from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city")
w_ordered = Window.partitionBy("region")

metrics = (
    df_stores
    .select(
        "id",
        "city",
        "region",
        F.first("sqft", ignorenulls=True).over(w).alias("first_known_sqft"),
        F.sum("sqft").over(w2).alias("running_sqft"),
        F.round((F.col("sqft") / F.sum("sqft").over(w3)), 4).alias("sqft_share"),
        F.sum("sqft").over(w_ordered).alias("region_sum_sqft"),
    )
)

result = (
    metrics
    .groupBy("city")
    .agg(
        F.percentile_approx("running_sqft", [0.25, 0.5, 0.75]).alias("running_sqft_quartiles"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_rows"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.countDistinct("region").alias("distinct_regions"),
        F.sum(F.when(F.col("city") == "Singapore", F.col("sqft_share")).otherwise(0)).alias("singapore_sqft_share"),
    )
    .orderBy(F.col("tokyo_rows").desc())
)
