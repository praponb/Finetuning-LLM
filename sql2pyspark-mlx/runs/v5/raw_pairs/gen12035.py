from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("carrier", "order_id").orderBy("delivered_date", "id")
w2 = Window.partitionBy("carrier").orderBy(F.col("is_lost").desc_nulls_first())
w3 = Window.partitionBy("order_id", "carrier").orderBy(F.col("event_seq").desc_nulls_first(), "id")
w4 = Window.partitionBy("carrier").orderBy(F.col("is_lost_dense_rank").desc_nulls_last(), "id")
w5 = Window.partitionBy("order_id").orderBy("row_num", "id")
w6 = Window.partitionBy("carrier").orderBy("is_lost_dense_rank", "id").rowsBetween(-6, Window.currentRow)
w7 = Window.partitionBy("order_id").orderBy(F.col("next_cost").desc(), "id")

filtered = (
    df_shipments
    .filter(F.col("carrier").eqNullSafe(F.lit("DHL")))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.when(F.col("status") == "LOST", 1).otherwise(0).alias("is_lost"),
        F.date_format("shipped_date", "yyyy-MM").alias("shipped_date_label"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_clean"),
    )
)

enriched = (
    filtered
    .filter(F.col("is_lost") >= 45)
    .withColumn("next_is_lost", F.lead("is_lost").over(w_part))
    .withColumn("next_cost", F.lead("cost").over(w_part))
    .withColumn("prev_cost", F.lag("cost").over(w_part))
    .withColumn("event_seq", F.row_number().over(w_part))
    .withColumn("is_lost_dense_rank", F.dense_rank().over(w2))
)

deduped = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

deduped_shipments = (
    deduped
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

deduped_shipments_2 = (
    deduped_shipments
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    deduped_shipments_2
    .select(
        "*",
        F.max("event_seq").over(w6).alias("last7_max_event_seq"),
        F.count("event_seq").over(w6).alias("last7_count_event_seq"),
        F.sum("event_seq").over(w6).alias("last7_sum_event_seq"),
        F.avg("event_seq").over(w6).alias("last7_avg_event_seq"),
        F.ntile(3).over(w7).alias("next_cost_bucket"),
    )
)

result = (
    scored
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "is_lost",
        "shipped_date_label",
        "next_cost",
        "prev_cost",
    )
)
