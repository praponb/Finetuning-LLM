from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "carrier").orderBy(F.col("shipped_date").asc_nulls_first(), "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("delivered_date").asc_nulls_first(), "id")

joined = (
    df_shipments
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.amount"), F.col("d.channel"))
)

ranked = (
    joined
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

deduped = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_dims = (
    deduped
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.discount"))
)

result = (
    with_dims
    .select(
        "id",
        "cost",
        "amount",
        "channel",
        "discount",
        F.when(F.col("channel").rlike("^(store|mobile)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("channel_zone"),
    )
)
