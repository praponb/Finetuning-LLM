from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("method").orderBy("method_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("method_length", "id")
w3 = Window.orderBy("running_sum_method_length", "id")
w4 = Window.partitionBy("status").orderBy(F.col("event_seq").desc(), "id")

cleaned = (
    df_payments
    .filter(~F.col("status").isin(["SUCCESS", "REFUNDED"]) & F.col("method").isNotNull())
    .select(
        "id",
        "method",
        "status",
        F.when(F.col("amount") >= 1410, "High").when(F.col("amount") >= 700, "Medium").otherwise("Low").alias("amount_tier"),
    )
)

valid = (
    cleaned
    .filter(F.col("method").isNotNull())
    .select(
        "*",
        F.length("method").alias("method_length"),
        F.lower("status").alias("status_lower"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.sum("method_length").over(w_hist).alias("running_sum_method_length"),
        F.avg("method_length").over(w_hist).alias("running_avg_method_length"),
        F.count("method_length").over(w_hist).alias("running_count_method_length"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

filtered = (
    metrics
    .filter(F.col("method").isNotNull())
    .select(
        "id",
        "method",
        "status",
        "amount_tier",
        "status_lower",
        "running_sum_method_length",
        "event_seq",
        F.concat("method", F.lit(", "), "status").alias("method_status"),
        F.coalesce("method", F.lit("N/A")).alias("method_clean"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.lag("running_sum_method_length", 1).over(w3).alias("prev_running_sum_method_length"),
        F.ntile(5).over(w3).alias("running_sum_method_length_bucket"),
    )
)

ranked = (
    enriched
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "method",
        "status",
        "amount_tier",
        "status_lower",
        "method_clean",
        "running_sum_method_length_bucket",
    )
)

result = (
    ranked
    .groupBy(
        "method",
        F.when(F.col("running_sum_method_length_bucket") >= 8, "LARGE").otherwise("SMALL").alias("running_sum_method_length_bucket_tier"),
    )
    .agg(
        F.count(F.when(F.col("method") == "card", 1)).alias("card_count"),
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("running_sum_method_length_bucket")).otherwise(0)).alias("success_running_sum_method_length_bucket"),
        F.percentile_approx("running_sum_method_length_bucket", 0.9).alias("running_sum_method_length_bucket_p90"),
        F.round(F.sum("running_sum_method_length_bucket") / F.nullif(F.count(F.when(F.col("status") == "SUCCESS", 1)), F.lit(0)), 2).alias("running_sum_method_length_bucket_per_success"),
        F.sum("running_sum_method_length_bucket").alias("total_running_sum_method_length_bucket"),
    )
    .orderBy(F.col("running_sum_method_length_bucket_p90").desc())
)
