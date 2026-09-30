from pyspark.sql import functions as F

valid_suppliers = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Global%") & (F.col("rating") < 2.2))
)

summary = (
    valid_suppliers
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("city") == "New York", 1).otherwise(0)).alias("new_york_count"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
    )
)

result = (
    summary
    .filter(F.col("stddev_rating") >= 65)
    .select("city", "stddev_rating")
)
