from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("created_at", "id", "k")
w2 = Window.partitionBy("customer_id").orderBy("created_at", "id", "k")
w3 = Window.orderBy("event_seq_2", "id", "k")
w4 = Window.partitionBy("customer_id")
w5 = Window.partitionBy("k")
w6 = Window.orderBy("created_at", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("event_seq").asc_nulls_first())
w8 = Window.partitionBy("customer_id", "k").orderBy("event_seq")

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("k", "v"),
    )
)

scored = (
    flattened
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("event_seq_2"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.lead("event_seq", 1).over(w3).alias("next_event_seq"),
        F.round((F.col("event_seq") / F.sum("event_seq").over(w4)), 4).alias("event_seq_share"),
        F.count("event_seq").over(w5).alias("k_count_event_seq"),
        F.last("event_seq_2", ignorenulls=True).over(w6).alias("last_known_event_seq_2"),
        F.rank().over(w7).alias("event_seq_rank"),
    )
)

latest = (
    metrics
    .withColumn("rn", F.rank().over(w8))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .select(
        "id",
        "coupon_codes",
        "k",
        "event_seq",
        "next_event_seq",
        "event_seq_share",
        "k_count_event_seq",
        "event_seq_rank",
        "rn",
    )
    .orderBy(F.col("event_seq_rank").desc_nulls_first())
)
