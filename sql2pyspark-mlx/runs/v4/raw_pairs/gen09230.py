from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w_ordered = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")
w6 = Window.partitionBy("country").orderBy(F.col("started_at").desc_nulls_last())
w7 = Window.orderBy(F.col("row_seq").desc(), "id")
w8 = Window.orderBy("row_seq", "id").rowsBetween(-5, -1)

scored = (
    df_sessions
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w_ordered))
    .withColumn("last_known_country_2", F.last("country", ignorenulls=True).over(w3))
    .withColumn("first_known_country", F.first("country", ignorenulls=True).over(w4))
    .withColumn("seq_no", F.row_number().over(w5))
)

ranked = (
    scored
    .withColumn("dedup_rank", F.rank().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

filtered = (
    ranked
    .filter((F.col("started_at") >= F.lit("2023-12-21 09:00:00").cast("timestamp")) & F.col("country").isNotNull())
    .select(
        "id",
        "started_at",
        "country",
        "row_seq",
        "last_known_country",
        F.when(F.col("country").isin(["JP", "UK"]), "Priority").otherwise("Other").alias("country_group"),
        F.coalesce("country", F.lit("N/A")).alias("country_clean"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.ntile(4).over(w7).alias("row_seq_bucket"),
        F.avg("row_seq").over(w8).alias("row_seq_avg_5_prior"),
    )
)

result = (
    enriched
    .groupBy(F.date_trunc("QUARTER", "started_at").alias("quarter_start"))
    .agg(
        F.round(F.sum("row_seq_avg_5_prior") / F.nullif(F.count(F.when(F.col("country") == "SG", 1)), F.lit(0)), 2).alias("row_seq_avg_5_prior_per_sg"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_rows"),
    )
)
