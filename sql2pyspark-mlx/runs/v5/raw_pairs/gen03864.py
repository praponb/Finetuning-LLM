from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("is_flash").desc_nulls_last())
w2 = Window.partitionBy("channel").orderBy("delivered_date", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("channel").orderBy(F.col("cost").desc(), "id")
w4 = Window.orderBy("shipped_date", "id")
w5 = Window.orderBy(F.col("cost").desc_nulls_last())
w6 = Window.orderBy("order_date", "id")

valid = (
    df_shipments
    .filter((F.col("shipped_date") >= F.lit("2023-05-22").cast("date")) & (F.col("shipped_date") <= "2023-12-01"))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.when(F.col("carrier") == "Flash", 1).otherwise(0).alias("is_flash"),
    )
)

ranked = (
    valid
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

base = (
    ranked
    .filter(F.col("carrier").isNotNull() & (F.col("is_flash") >= 35))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.datediff(F.current_date(), "delivered_date").alias("days_since_delivered_date"),
    )
)

with_dims = (
    base
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.order_date"), F.col("p.channel"))
)

with_windows = (
    with_dims
    .filter(F.col("delivered_date") > "2025-05-27")
    .withColumn("cost_avg_6_prior", F.avg("cost").over(w2))
    .withColumn("cost_bucket", F.ntile(10).over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
    .withColumn("cost_rank", F.rank().over(w5))
    .withColumn("next_cost", F.lead("cost", 1, 0).over(w6))
)

result = (
    with_windows
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "order_date",
        "channel",
        "cost_avg_6_prior",
        "seq_no",
        "cost_rank",
        F.when(F.col("channel").rlike("^(store|web)$"), "GROUP_B").otherwise("EXTENDED").alias("channel_zone"),
    )
)
