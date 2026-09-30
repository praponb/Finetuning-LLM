from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("days_shipped_date_to_delivered_date", "id")
w2 = Window.partitionBy("status").orderBy(F.col("cost").desc(), "id")
w3 = Window.orderBy(F.col("days_shipped_date_to_delivered_date").desc_nulls_last())
w4 = Window.partitionBy("status", "order_id").orderBy("delivered_date", "id")
w5 = Window.partitionBy("order_id").orderBy(F.col("shipped_date").desc(), "id")
w6 = Window.partitionBy("order_id").orderBy(F.col("cost_bucket").desc_nulls_first())
w7 = Window.partitionBy("order_id").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("order_id").orderBy("latest_rank", "id")
w9 = Window.partitionBy("order_id").orderBy("days_shipped_date_to_delivered_date", "id").rowsBetween(-3, Window.currentRow)
w10 = Window.orderBy("days_shipped_date_to_delivered_date_percent_rank", "id")

filtered = (
    df_shipments
    .filter(F.col("shipped_date") >= F.lit("2023-06-30").cast("date"))
    .select(
        "*",
        F.datediff("delivered_date", "shipped_date").alias("days_shipped_date_to_delivered_date"),
        F.concat_ws("-", "carrier", "status").alias("carrier_status_key"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("carrier_clean"),
    )
)

deduped = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    deduped
    .select(
        "id",
        "order_id",
        "delivered_date",
        "days_shipped_date_to_delivered_date",
        "carrier_status_key",
        "latest_rank",
        F.ntile(10).over(w2).alias("cost_bucket"),
        F.percent_rank().over(w3).alias("days_shipped_date_to_delivered_date_percent_rank"),
        F.lead("days_shipped_date_to_delivered_date", 1).over(w4).alias("next_days_shipped_date_to_delivered_date"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

latest = (
    enriched
    .withColumn("rn", F.rank().over(w6))
    .filter(F.col("rn") == 1)
)

with_windows = (
    latest
    .withColumn("running_row_seq", F.sum("row_seq").over(w7))
    .withColumn("next_days_shipped_date_to_delivered_date_2", F.lead("days_shipped_date_to_delivered_date", 1).over(w8))
)

base = (
    with_windows
    .filter(F.col("delivered_date") >= F.lit("2023-03-20").cast("date"))
    .select(
        "id",
        "order_id",
        "days_shipped_date_to_delivered_date",
        "latest_rank",
        "cost_bucket",
        "days_shipped_date_to_delivered_date_percent_rank",
        "next_days_shipped_date_to_delivered_date",
        "row_seq",
        "running_row_seq",
        F.round((F.col("cost_bucket") / F.col("latest_rank")), 2).alias("cost_bucket_per_latest_rank"),
    )
)

metrics = (
    base
    .filter(F.col("latest_rank") == 1)
    .select(
        "*",
        F.avg("cost_bucket").over(w9).alias("cost_bucket_avg_3"),
        F.ntile(3).over(w10).alias("days_shipped_date_to_delivered_date_percent_rank_bucket"),
    )
)

result = (
    metrics
    .select(
        "id",
        "days_shipped_date_to_delivered_date",
        "cost_bucket",
        "next_days_shipped_date_to_delivered_date",
        "row_seq",
        "running_row_seq",
        "cost_bucket_per_latest_rank",
        "cost_bucket_avg_3",
        "days_shipped_date_to_delivered_date_percent_rank_bucket",
    )
    .orderBy(
        F.col("days_shipped_date_to_delivered_date_percent_rank_bucket").asc_nulls_first(),
    )
)
