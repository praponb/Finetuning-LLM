from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_sessions
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("last_known_browser", F.last("browser", ignorenulls=True).over(w2))
    .withColumn("first_known_browser", F.first("browser", ignorenulls=True).over(w3))
)

result = (
    scored
    .groupBy(
        "browser",
        F.when(F.col("row_seq") >= 8, "LARGE").otherwise("SMALL").alias("row_seq_tier"),
        F.date_trunc("MONTH", "started_at").alias("activity_month"),
    )
    .agg(
        F.count_if(F.col("country") == "DE").alias("de_events"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.avg(F.when(F.col("country") == "US", F.col("row_seq"))).alias("us_avg_row_seq"),
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
    )
)
