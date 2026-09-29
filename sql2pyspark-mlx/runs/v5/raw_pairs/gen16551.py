from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("created_at_period", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("key").orderBy("created_at_period", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("created_at").desc(), "id", "key")
w_recent = Window.partitionBy("key").orderBy("event_seq", "id", "key")
w5 = Window.partitionBy("key").orderBy(F.col("event_seq").desc_nulls_last())
w6 = Window.partitionBy("key").orderBy(F.col("event_seq").desc(), "id", "key")
w7 = Window.partitionBy("key").orderBy(F.col("event_seq_2").desc(), "id", "key")

exploded = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "attr_value"),
    )
)

cleaned = (
    exploded
    .filter((F.col("created_at") >= F.lit("2025-03-02").cast("date")) & F.hour("created_at").between(6, 9))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "key",
        F.array_contains("coupon_codes", "WELCOME5").alias("is_welcome5"),
        F.date_trunc("month", "created_at").alias("created_at_period"),
    )
)

enriched = (
    cleaned
    .select(
        "id",
        "coupon_codes",
        "key",
        "is_welcome5",
        "created_at_period",
        F.first("key", ignorenulls=True).over(w_running).alias("first_known_key"),
        F.first("key", ignorenulls=True).over(w2).alias("first_known_key_2"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "coupon_codes",
        "key",
        "first_known_key",
        "first_known_key_2",
        "event_seq",
        F.lead("event_seq").over(w_recent).alias("next_event_seq"),
        F.lag("event_seq").over(w_recent).alias("prev_event_seq"),
        F.row_number().over(w_recent).alias("event_seq_2"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.rank().over(w5).alias("event_seq_rank"),
        F.ntile(5).over(w6).alias("event_seq_bucket"),
        F.ntile(4).over(w7).alias("event_seq_2_bucket"),
        F.row_number().over(w6).alias("seq_no"),
    )
)

result = (
    metrics
    .select(
        "id",
        "coupon_codes",
        "key",
        "first_known_key",
        "first_known_key_2",
        "prev_event_seq",
        "event_seq_2",
        "event_seq_2_bucket",
        F.when(F.col("key").rlike("^(device|gift)$"), "CORE").otherwise("GROUP_B").alias("key_zone"),
    )
)
