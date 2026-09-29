from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("city").orderBy(F.col("sqft_avg_3").asc_nulls_last(), "id")

filtered = (
    df_stores
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-08-19"))) & F.col("region").isNotNull() & F.col("opened_date").between("2024-01-01", "2025-05-12"))
    .select(
        "*",
        F.trim("store_name").alias("clean_store_name"),
        F.coalesce("region", F.lit("OTHER")).alias("resolved_region"),
    )
)

scored = (
    filtered
    .withColumn("first_known_region", F.first("region", ignorenulls=True).over(w))
    .withColumn("sqft_avg_3", F.avg("sqft").over(w2))
)

ranked = (
    scored
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .groupBy(
        F.when(F.col("sqft_avg_3") >= 12774, "PREMIUM").otherwise("STANDARD").alias("sqft_avg_3_tier"),
    )
    .agg(
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("city") == "New York", 1)), F.lit(0)), 2).alias("sqft_per_new_york"),
        F.count(F.when(F.col("region") == "East", 1)).alias("east_rows"),
        F.size(F.collect_set("region")).alias("num_regions"),
    )
)
