from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id")
w2 = Window.orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_stores
    .filter(F.col("region").isNotNull() & (F.col("sqft") <= 5931))
    .withColumn("store_name_normalized", F.upper(F.trim("store_name")))
    .withColumn("region_clean", F.coalesce("region", F.lit("N/A")))
)

cleaned_stores = (
    cleaned
    .filter((F.col("opened_date") >= F.lit("2023-08-24").cast("date")) & F.col("region").isNotNull())
    .withColumn("region_title", F.initcap(F.trim("region")))
    .withColumn("resolved_region", F.coalesce("region", F.lit("UNKNOWN")))
)

enriched = (
    cleaned_stores
    .withColumn("sqft_bucket", F.ntile(3).over(w))
    .withColumn("cumulative_sqft", F.sum("sqft").over(w2))
)

result = (
    enriched
    .withColumn("week_start", F.date_trunc("week", "opened_date"))
    .withColumn("sqft_tier", F.when(F.col("sqft") >= 17312, "LARGE").otherwise("SMALL"))
    .rollup("week_start", "city", "sqft_tier")
    .agg(
        F.percentile_approx("cumulative_sqft", 0.5).alias("cumulative_sqft_p50"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.bit_or("sqft").alias("sqft_bit_or"),
        F.bit_and("sqft").alias("sqft_bit_and"),
        F.grouping("week_start").alias("grouping_id"),
    )
    .filter(F.col("cumulative_sqft_p50") < 0)
    .orderBy(F.col("week_start").desc())
)
