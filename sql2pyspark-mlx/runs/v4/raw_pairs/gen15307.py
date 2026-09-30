from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")
w4 = Window.partitionBy("country")
w_part = Window.orderBy(F.col("started_at").desc(), "id")
w_recent = Window.partitionBy("country").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w7 = Window.partitionBy("device").orderBy(F.col("row_seq").desc_nulls_last())

enriched = (
    df_sessions
    .withColumn("last_known_browser", F.last("browser", ignorenulls=True).over(w))
    .withColumn("first_known_browser", F.first("browser", ignorenulls=True).over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
)

enriched_sessions = (
    enriched
    .select(
        "*",
        F.first("row_seq", ignorenulls=True).over(w).alias("first_known_row_seq"),
        F.round((F.col("row_seq") / F.sum("row_seq").over(w4)), 4).alias("row_seq_share"),
        F.row_number().over(w_part).alias("seq_no"),
        F.sum("row_seq").over(w_recent).alias("row_seq_sum_30d"),
    )
)

base = (
    enriched_sessions
    .filter((F.col("started_at") > F.lit("2023-07-13").cast("date")) & F.col("browser").isNotNull() & F.col("country").isin(["DE", "US", "JP"]))
    .withColumn("device_group", F.when(F.col("device").isin(["tablet", "desktop"]), "Priority").otherwise("Other"))
    .withColumn("browser_filled", F.coalesce("browser", F.lit("unassigned")))
    .withColumn("browser_clean", F.coalesce("browser", F.lit("N/A")))
)

deduped = (
    base
    .withColumn("dedup_rank", F.rank().over(w7))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .rollup(
        F.date_trunc("YEAR", "started_at").alias("year_start"),
        F.when(F.col("row_seq_sum_30d") >= 40, "PREMIUM").otherwise("STANDARD").alias("row_seq_sum_30d_tier"),
    )
    .agg(
        F.max("first_known_row_seq").alias("max_first_known_row_seq"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.max("started_at").alias("last_started_at"),
        F.round(F.sum("seq_no") / F.nullif(F.count(F.when(F.col("country") == "DE", 1)), F.lit(0)), 2).alias("seq_no_per_de"),
        F.sum(F.when(F.col("country") == "JP", F.col("seq_no")).otherwise(0)).alias("jp_seq_no"),
    )
)
