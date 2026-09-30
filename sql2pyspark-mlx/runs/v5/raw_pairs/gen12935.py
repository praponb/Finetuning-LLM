from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("delivered_date").desc_nulls_last(), "id")
w2 = Window.orderBy(F.col("cost").desc(), "id")
w_running = Window.orderBy("cost")
w4 = Window.orderBy("shipped_date_plus_months", "id")
w5 = Window.orderBy("cost_bucket")
w6 = Window.orderBy("cost_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("event_seq", "id").rowsBetween(-5, Window.currentRow)
w8 = Window.orderBy(F.col("next_cost").desc(), "id")
w_grp = Window.orderBy("cost_bucket_rank", "id").rowsBetween(-3, -1)
w10 = Window.orderBy("cost_bucket_rank", "id")

base = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & (F.col("carrier").eqNullSafe(F.lit("FedEx"))))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.concat_ws("/", "carrier", "status").alias("carrier_status_key"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_filled"),
    )
)

deduped = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

cleaned = (
    deduped
    .filter(F.col("rank_in_group") >= 1)
    .select(
        "id",
        "cost",
        "carrier_filled",
        F.add_months("shipped_date", 1).alias("shipped_date_plus_months"),
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_clean"),
    )
)

metrics = (
    cleaned
    .filter(F.col("shipped_date_plus_months") < "2024-01-01")
    .select(
        "*",
        F.ntile(3).over(w2).alias("cost_bucket"),
        F.cume_dist().over(w_running).alias("cost_cume_dist"),
    )
)

metrics_shipments = (
    metrics
    .select(
        "id",
        "shipped_date_plus_months",
        "cost_bucket",
        F.lead("cost_cume_dist").over(w4).alias("next_cost_cume_dist"),
        F.lag("cost").over(w4).alias("prev_cost"),
        F.lead("cost").over(w4).alias("next_cost"),
        F.row_number().over(w4).alias("event_seq"),
        F.rank().over(w5).alias("cost_bucket_rank"),
    )
)

scored = (
    metrics_shipments
    .select(
        "id",
        "shipped_date_plus_months",
        "next_cost_cume_dist",
        "prev_cost",
        "cost_bucket_rank",
        F.last("cost_bucket", ignorenulls=True).over(w6).alias("last_known_cost_bucket"),
        F.avg("event_seq").over(w_recent).alias("event_seq_avg_5"),
        F.ntile(3).over(w8).alias("next_cost_bucket"),
        F.avg("cost_bucket_rank").over(w_grp).alias("cost_bucket_rank_avg_3_prior"),
        F.row_number().over(w10).alias("seq_no"),
    )
)

result = (
    scored
    .select(
        "id",
        "shipped_date_plus_months",
        "next_cost_cume_dist",
        "prev_cost",
        "cost_bucket_rank",
        "last_known_cost_bucket",
        "event_seq_avg_5",
        "next_cost_bucket",
        "cost_bucket_rank_avg_3_prior",
    )
)
