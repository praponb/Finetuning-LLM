from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy("order_date", "id").rowsBetween(-2, Window.currentRow)
w_part_2 = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").asc_nulls_first())
w3 = Window.partitionBy("store_id").orderBy("order_date", "id")

eligible = (
    df_orders
    .alias("s")
    .join(df_payments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.method") == "promptpay"), "left_anti")
)

scored = (
    eligible
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "discount",
        F.avg("shipping_fee").over(w_part).alias("shipping_fee_avg_2"),
        F.percent_rank().over(w_part_2).alias("shipping_fee_percent_rank"),
        F.lag("amount", 1).over(w3).alias("prev_amount"),
    )
)

valid = (
    scored
    .filter((F.col("order_date") > F.lit("2023-07-07").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("status_channel_key", F.concat_ws("-", "status", "channel"))
)

kept_keys = (
    valid
    .select("store_id")
    .exceptAll(
        df_orders
        .filter(F.col("amount") < 1100)
        .select("store_id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("customer_id", "store_id")
    .pivot("status", ["RETURNED", "PENDING", "SHIPPED"])
    .agg(F.avg("prev_amount"))
)
