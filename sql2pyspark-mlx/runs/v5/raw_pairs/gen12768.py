from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("delivered_date_month_end").desc_nulls_last(), "id")
w3 = Window.partitionBy("status").orderBy(F.col("running_sum_cost").desc(), "id")
w4 = Window.partitionBy("status").orderBy("delivered_date_month_end", "id")
w5 = Window.partitionBy("status")
w6 = Window.orderBy("event_seq", "id").rowsBetween(-5, Window.currentRow)

with_windows = (
    df_shipments
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "status",
        F.avg("cost").over(w).alias("running_avg_cost"),
        F.min("cost").over(w).alias("running_min_cost"),
        F.sum("cost").over(w).alias("running_sum_cost"),
        F.max("cost").over(w).alias("running_max_cost"),
    )
)

base = (
    with_windows
    .filter(F.col("running_max_cost").between(31, 60))
    .withColumn("delivered_date_month_end", F.last_day("delivered_date"))
)

ranked = (
    base
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

filtered = (
    ranked
    .filter(F.col("row_num") < 1)
    .select("*", F.substring("status", 1, 2).alias("status_prefix"))
)

ranked_shipments = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "status", "delivered_date_month_end")
)

metrics = (
    ranked_shipments
    .select(
        "id",
        "status",
        F.row_number().over(w4).alias("event_seq"),
        F.row_number().over(w4).alias("row_seq"),
        F.row_number().over(w4).alias("event_seq_2"),
    )
)

scored = (
    metrics
    .filter(F.col("row_seq") > 4)
    .select(
        "*",
        F.max("event_seq_2").over(w5).alias("status_max_event_seq_2"),
        F.avg("row_seq").over(w6).alias("row_seq_avg_5"),
    )
)

result = (
    scored
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_count"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.round(F.sum("event_seq") / F.nullif(F.count(F.when(F.col("status") == "IN_TRANSIT", 1)), F.lit(0)), 2).alias("event_seq_per_in_transit"),
    )
    .filter(F.col("delivered_count") > 1)
)
