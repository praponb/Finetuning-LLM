from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy(F.col("store_name_length").asc_nulls_first())

valid = (
    df_stores
    .filter(~F.col("city").isin(["Berlin", "London"]) & (~F.col("region").eqNullSafe(F.lit("West"))))
    .withColumn("clean_store_name", F.trim("store_name"))
    .withColumn("region_clean", F.coalesce("region", F.lit("DIRECT")))
)

cleaned = (
    valid
    .filter(F.col("opened_date") >= F.to_date(F.lit("2023-04-14")))
    .select("*", F.length("store_name").alias("store_name_length"))
)

scored = (
    cleaned
    .select(
        "*",
        F.ntile(10).over(w).alias("sqft_bucket"),
        F.sum("store_name_length").over(w2).alias("running_store_name_length"),
    )
)

ranked = (
    scored
    .withColumn("rn", F.rank().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "store_name",
        "city",
        "clean_store_name",
        "region_clean",
        "store_name_length",
    )
)

result = (
    ranked
    .withColumn("store_name_length_tier", F.when(F.col("store_name_length") >= 82, "LARGE").otherwise("SMALL"))
    .cube("store_name_length_tier")
    .agg(
        F.sum("store_name_length").alias("total_store_name_length"),
        F.count_if(F.col("city") == "Bangkok").alias("bangkok_events"),
        F.round(F.sum("store_name_length") / F.nullif(F.count(F.when(F.col("city") == "New York", 1)), F.lit(0)), 2).alias("store_name_length_per_new_york"),
        F.avg(F.when(F.col("city") == "Bangkok", F.col("store_name_length"))).alias("bangkok_avg_store_name_length"),
        F.round(F.sum("store_name_length") / F.nullif(F.count(F.when(F.col("city") == "London", 1)), F.lit(0)), 2).alias("store_name_length_per_london"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("total_store_name_length") > 0)
)
