from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_sessions
    .filter(F.col("country").isNotNull() & (F.col("country") == "TH"))
    .select(
        "*",
        F.when(F.col("country") == "JP", 1).otherwise(0).alias("is_jp"),
        F.coalesce("country", F.lit("N/A")).alias("resolved_country"),
    )
)

flagged = (
    valid
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .rollup(
        F.date_trunc("YEAR", "started_at").alias("year_start"),
        F.when(F.col("is_new_session") >= 1, "HIGH").otherwise("LOW").alias("is_new_session_tier"),
    )
    .agg(
        F.percentile_approx("is_jp", 0.5).alias("is_jp_p50"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_rows"),
        F.countDistinct("country").alias("distinct_countries"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
    )
    .filter(F.col("distinct_customer_ids") > 1)
)
