from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("order_date").desc(), "id")
w2 = Window.partitionBy("store_id")
w3 = Window.partitionBy("store_id").orderBy(F.col("amount").desc(), "id")
w4 = Window.partitionBy("status").orderBy("order_date", "id")

base = (
    df_orders
    .filter(F.col("channel") == "mobile")
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
        (F.col("amount") * F.col("discount")).alias("amount_times_discount"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_filled_2"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.min("amount_times_discount").over(w2).alias("store_id_min_amount_times_discount"),
        F.row_number().over(w3).alias("row_seq_2"),
    )
)

latest = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "amount",
        "amount_times_discount",
        "channel_filled_2",
        "store_id_min_amount_times_discount",
        "rank_in_group",
    )
    .orderBy(F.col("store_id").asc_nulls_last(), "order_date")
)
