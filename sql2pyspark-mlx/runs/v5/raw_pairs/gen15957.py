from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status", "method").orderBy(F.col("paid_at").desc(), "id")
w3 = Window.orderBy(F.col("paid_at_period_2").desc(), "id")
w4 = Window.partitionBy("order_id").orderBy(F.col("row_seq_2").desc(), "id")

valid = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.date_trunc("MONTH", "paid_at").alias("paid_at_period"),
        F.date_trunc("month", "paid_at").alias("paid_at_period_2"),
    )
)

scored = (
    valid
    .select(
        "id",
        "order_id",
        "paid_at",
        "paid_at_period_2",
        F.first("method", ignorenulls=True).over(w).alias("first_known_method"),
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w3).alias("row_seq_2"),
    )
)

latest = (
    scored
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .select("id", "row_seq_2")
)

base = (
    latest
    .filter(F.col("row_seq_2") > 5)
    .select("*", F.round("row_seq_2", 1).alias("row_seq_2_rounded"))
)

result = (
    base
    .withColumn("row_seq_2_tier", F.when(F.col("row_seq_2") >= 7, "PREMIUM").otherwise("STANDARD"))
    .withColumn("row_seq_2_rounded_tier", F.when(F.col("row_seq_2_rounded") >= 67, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["row_seq_2_tier", "row_seq_2_rounded_tier"], ["row_seq_2_tier"]],
        "row_seq_2_tier",
        "row_seq_2_rounded_tier",
    )
    .agg(
        F.round(F.avg("row_seq_2"), 2).alias("avg_row_seq_2"),
        F.min("row_seq_2").alias("min_row_seq_2"),
        F.percentile_approx("row_seq_2_rounded", 0.95).alias("row_seq_2_rounded_p95"),
        F.grouping("row_seq_2_tier").alias("gid"),
    )
)
