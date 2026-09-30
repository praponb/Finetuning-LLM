from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc())
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city").orderBy(F.col("sqft_avg_6_prior").asc_nulls_last())
w6 = Window.partitionBy("region").orderBy("sqft_share", "id")
w7 = Window.partitionBy("region", "city").orderBy(F.col("sqft").desc(), "id")
w8 = Window.partitionBy("city", "region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("region").orderBy("sqft_avg_6_prior_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("region")

ranked = (
    df_stores
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched = (
    ranked
    .select(
        "*",
        F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4).alias("sqft_share"),
        F.avg("sqft").over(w3).alias("sqft_avg_6_prior"),
        F.last("sqft", ignorenulls=True).over(w4).alias("last_known_sqft"),
    )
)

enriched_stores = (
    enriched
    .withColumn("sqft_avg_6_prior_percent_rank", F.percent_rank().over(w5))
    .withColumn("sqft_share_bucket", F.ntile(4).over(w6))
    .withColumn("sqft_bucket", F.ntile(10).over(w7))
    .withColumn("running_sqft_avg_6_prior", F.sum("sqft_avg_6_prior").over(w8))
)

metrics = (
    enriched_stores
    .withColumn("last_known_region", F.last("region", ignorenulls=True).over(w9))
    .withColumn("region_avg_sqft", F.avg("sqft").over(w_grp))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("sqft_share_bucket") >= 8, "LARGE").otherwise("SMALL").alias("sqft_share_bucket_tier"),
    )
    .agg(
        F.bit_or("sqft").alias("sqft_bit_or"),
        F.count(F.when(F.col("region") == "East", 1)).alias("east_rows"),
    )
)
