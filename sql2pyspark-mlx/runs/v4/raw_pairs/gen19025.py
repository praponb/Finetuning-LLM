from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id", "country").orderBy(F.col("seq_no").desc_nulls_last(), "id")

with_windows = (
    df_sessions
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("first_known_browser", F.first("browser", ignorenulls=True).over(w2))
    .withColumn("first_known_browser_2", F.first("browser", ignorenulls=True).over(w3))
)

latest = (
    with_windows
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

cleaned = (
    latest
    .filter(F.col("seq_no").between(4, 8))
    .select(
        "id",
        "customer_id",
        "started_at",
        "country",
        "seq_no",
        "first_known_browser",
        "first_known_browser_2",
        F.substring("browser", 1, 4).alias("browser_prefix"),
        F.coalesce("country", F.lit("UNKNOWN")).alias("resolved_country"),
    )
)

result = (
    cleaned
    .groupBy(
        "customer_id",
        F.when(F.col("seq_no") >= 6, "LARGE").otherwise("SMALL").alias("seq_no_tier"),
        "seq_no",
    )
    .agg(
        F.max(F.when(F.col("country") == "UK", F.col("seq_no"))).alias("uk_max_seq_no"),
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
        F.sum(F.when(F.col("country") == "SG", F.col("seq_no")).otherwise(0)).alias("sg_seq_no"),
        F.count_if(F.col("country") == "US").alias("us_events"),
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles_2"),
    )
)
