from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("status").orderBy("delivered_date", "id")
w3 = Window.partitionBy("carrier").orderBy(F.col("event_seq").desc(), "id")
w4 = Window.partitionBy("order_id", "carrier").orderBy(F.col("event_seq").asc_nulls_first(), "id")
w_recent = Window.orderBy("event_seq", "id").rowsBetween(-3, Window.currentRow)
w_part = Window.orderBy("row_num", "id")
w7 = Window.orderBy("last4_avg_event_seq", "id")
w8 = Window.orderBy(F.col("last4_count_event_seq").desc(), "id")
w9 = Window.orderBy(F.col("last4_count_event_seq").desc())
w10 = Window.orderBy("last4_count_event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_shipments
    .filter(F.col("carrier").isin(["Kerry", "UPS", "FedEx"]))
    .select(
        "id",
        "order_id",
        "carrier",
        F.avg("cost").over(w).alias("cost_avg_6_prior"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

latest = (
    enriched
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

deduped = (
    latest
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("id", "event_seq", "row_num")
)

with_windows = (
    deduped
    .select(
        "*",
        F.count("event_seq").over(w_recent).alias("last4_count_event_seq"),
        F.avg("event_seq").over(w_recent).alias("last4_avg_event_seq"),
        F.sum("event_seq").over(w_recent).alias("last4_sum_event_seq"),
        F.lag("event_seq", 1).over(w_part).alias("prev_event_seq"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "last4_count_event_seq",
        F.lead("last4_sum_event_seq", 1).over(w7).alias("next_last4_sum_event_seq"),
        F.ntile(3).over(w7).alias("last4_avg_event_seq_bucket"),
        F.row_number().over(w8).alias("row_seq"),
    )
)

cleaned = (
    scored
    .filter(F.col("last4_avg_event_seq_bucket") != 5)
    .select(
        "*",
        (F.col("last4_avg_event_seq_bucket") - F.col("next_last4_sum_event_seq")).alias("last4_avg_event_seq_bucket_minus_next_last4_sum_event_seq"),
        F.when(F.col("next_last4_sum_event_seq") >= 35, "High").when(F.col("next_last4_sum_event_seq") >= 20, "Medium").otherwise("Low").alias("next_last4_sum_event_seq_band"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.cume_dist().over(w9).alias("last4_count_event_seq_cume_dist"),
        F.last("last4_avg_event_seq_bucket", ignorenulls=True).over(w10).alias("last_known_last4_avg_event_seq_bucket"),
        F.sum("row_seq").over(w11).alias("cumulative_row_seq"),
    )
)

result = (
    metrics
    .select(
        "id",
        "last4_avg_event_seq_bucket_minus_next_last4_sum_event_seq",
        "next_last4_sum_event_seq",
        "last4_count_event_seq_cume_dist",
    )
    .unpivot(
        ids=["id"],
        values=["last4_avg_event_seq_bucket_minus_next_last4_sum_event_seq", "next_last4_sum_event_seq", "last4_count_event_seq_cume_dist"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
)
