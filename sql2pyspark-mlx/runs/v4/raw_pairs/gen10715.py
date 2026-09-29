from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment")
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("country").orderBy(F.col("signup_date").desc_nulls_last(), "id")
w4 = Window.partitionBy("city").orderBy(F.col("age").desc_nulls_first(), "id")

cleaned = (
    df_customers
    .filter(F.col("segment").isin(["SMB", "Corporate"]))
    .withColumn("age_filled", F.coalesce("age", F.lit(0)))
    .withColumn("age_rounded", F.round("age", 0))
    .withColumn("segment_clean", F.coalesce("segment", F.lit("N/A")))
)

enriched = (
    cleaned
    .withColumn("credit_limit_share", F.round((F.col("credit_limit") / F.sum("credit_limit").over(w)), 4))
    .withColumn("credit_limit_share_2", F.round((F.col("credit_limit") / F.sum("credit_limit").over(w2)), 4))
    .withColumn("segment_max_credit_limit", F.max("credit_limit").over(w))
)

deduped = (
    enriched
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

ranked = (
    deduped
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

stats = (
    ranked
    .groupBy("segment")
    .agg(
        F.percentile_approx("credit_limit_share", [0.25, 0.5, 0.75]).alias("credit_limit_share_quartiles"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_rows"),
        F.stddev_samp("credit_limit").alias("credit_limit_stddev"),
    )
    .filter(F.col("uk_rows") > 1)
)

result = (
    stats
    .select("segment", "credit_limit_share_quartiles", "uk_rows", "credit_limit_stddev")
    .orderBy(F.col("credit_limit_stddev").desc_nulls_last())
)
