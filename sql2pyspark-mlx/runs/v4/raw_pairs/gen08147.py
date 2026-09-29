from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("country").orderBy("started_at_day", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("started_at_day", "id")
w3 = Window.partitionBy("device").orderBy("started_at_day", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("browser", "country").orderBy("seq_no", "id")

base = (
    df_sessions
    .filter((F.col("started_at") > F.to_timestamp(F.lit("2023-12-14 07:00:00"))) & F.col("country").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country",
        F.col("started_at").cast("date").alias("started_at_day"),
        F.upper("device").alias("device_upper"),
        F.coalesce("browser", F.lit("OTHER")).alias("browser_clean"),
    )
)

with_windows = (
    base
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w_hist))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("last_known_browser", F.last("browser", ignorenulls=True).over(w_hist))
    .withColumn("first_known_country", F.first("country", ignorenulls=True).over(w3))
)

ranked = (
    with_windows
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .cube(
        F.when(F.col("seq_no") >= 9, "LARGE").otherwise("SMALL").alias("seq_no_tier"),
        F.date_trunc("QUARTER", "started_at_day").alias("quarter_start"),
    )
    .agg(
        F.size(F.collect_set("browser")).alias("num_browsers"),
        F.sum(F.when(F.col("browser") == "Edge", F.col("seq_no")).otherwise(0)).alias("edge_seq_no"),
        F.sum(F.when(F.col("country") == "DE", F.col("seq_no"))).alias("de_total_seq_no"),
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
        F.sum(F.when(F.col("device") == "mobile", F.col("seq_no"))).alias("mobile_total_seq_no"),
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
    )
    .filter(F.col("mobile_total_seq_no") >= 0)
    .orderBy("edge_seq_no")
)
