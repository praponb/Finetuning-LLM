from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("delivered_date").asc_nulls_first(), "id")
w_running = Window.orderBy(F.col("cost").desc(), "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("shipped_date_month").desc(), "id")

cleaned = (
    df_shipments
    .filter(F.col("shipped_date").between("2024-10-19", "2025-03-01"))
    .select(
        "*",
        F.month("shipped_date").alias("shipped_date_month"),
        F.date_add("shipped_date", 15).alias("shipped_date_plus_15d"),
    )
)

latest = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

scored = (
    latest
    .withColumn("cost_bucket", F.ntile(4).over(w_running))
    .withColumn("seq_no", F.row_number().over(w3))
)

summary = (
    scored
    .groupBy(
        F.date_trunc("MONTH", "shipped_date").alias("activity_month"),
        F.date_trunc("QUARTER", "shipped_date_plus_15d").alias("quarter_start"),
        F.when(F.col("seq_no") >= 7, "LARGE").otherwise("SMALL").alias("seq_no_tier"),
    )
    .agg(
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
        F.count("*").alias("num_rows"),
        F.sum(F.when(F.col("status") == "RETURNED", F.col("seq_no")).otherwise(0)).alias("returned_seq_no"),
        F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_count"),
    )
    .filter(F.col("delivered_count") >= 1)
)

result = (
    summary
    .select(
        "activity_month",
        "quarter_start",
        "seq_no_tier",
        "seq_no_quartiles",
        "num_rows",
        "returned_seq_no",
        "delivered_count",
    )
    .orderBy(F.col("returned_seq_no").desc())
)
