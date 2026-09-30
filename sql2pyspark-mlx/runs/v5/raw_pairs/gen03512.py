from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("cost").desc_nulls_first(), "id")
w_part = Window.partitionBy("carrier").orderBy("delivered_date", "id")
w3 = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")
w4 = Window.partitionBy("carrier").orderBy(F.col("cost_bucket").desc_nulls_first(), "id")
w5 = Window.partitionBy("carrier").orderBy(F.col("next_cost").desc_nulls_last(), "id")
w6 = Window.partitionBy("carrier").orderBy(F.col("delivered_date").desc_nulls_first(), "id")
w7 = Window.partitionBy("carrier").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_shipments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "carrier", "delivered_date", "cost")
)

metrics = (
    ranked
    .filter(F.col("cost") > 37)
    .select(
        "id",
        "carrier",
        "delivered_date",
        F.lag("cost").over(w_part).alias("prev_cost"),
        F.lead("cost").over(w_part).alias("next_cost"),
        F.row_number().over(w_part).alias("event_seq"),
        F.ntile(4).over(w3).alias("cost_bucket"),
    )
)

deduped = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

latest = (
    deduped
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("id", "carrier", "delivered_date", "next_cost", "event_seq", "cost_bucket")
)

filtered = (
    latest
    .filter((F.col("delivered_date") >= F.lit("2023-07-14").cast("date")) & F.col("carrier").isNotNull())
    .withColumn("event_seq_filled", F.coalesce("event_seq", F.lit(0)))
)

latest_shipments = (
    filtered
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
)

metrics_shipments = (
    latest_shipments
    .select(
        "id",
        "next_cost",
        "cost_bucket",
        "event_seq_filled",
        F.count("cost_bucket").over(w7).alias("running_count_cost_bucket"),
        F.sum("cost_bucket").over(w7).alias("running_sum_cost_bucket"),
        F.max("cost_bucket").over(w7).alias("running_max_cost_bucket"),
        F.avg("cost_bucket").over(w7).alias("running_avg_cost_bucket"),
    )
)

result = (
    metrics_shipments
    .groupBy(
        F.when(F.col("cost_bucket") >= 7, "HIGH").otherwise("LOW").alias("cost_bucket_tier"),
    )
    .agg(
        F.round(F.avg("running_count_cost_bucket"), 2).alias("avg_running_count_cost_bucket"),
        F.round(F.avg("running_sum_cost_bucket"), 2).alias("avg_running_sum_cost_bucket"),
        F.count("*").alias("event_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("event_count_2"),
    )
    .orderBy(F.col("avg_running_sum_cost_bucket").desc_nulls_first())
)
