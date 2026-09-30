from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())

valid = (
    df_orders
    .filter(F.col("order_date") >= F.lit("2023-10-28").cast("date"))
    .withColumn("days_since_order_date", F.datediff(F.current_date(), "order_date"))
)

eligible = (
    valid
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.unit_price") > 260), "left_anti")
)

with_last = (
    eligible
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price").alias("order_item_unit_price"))
)

filtered = (
    with_last
    .filter(F.col("order_date") > F.lit("2024-04-21").cast("date"))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "days_since_order_date",
        F.col("discount").cast("int").alias("discount_int"),
        (F.col("discount") - F.col("days_since_order_date")).alias("discount_minus_days_since_order_date"),
        F.coalesce("channel", F.lit("NONE")).alias("resolved_channel"),
    )
)

result = (
    filtered
    .groupBy(F.date_trunc("WEEK", "order_date").alias("week_start"))
    .agg(
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
        F.stddev_samp("discount").alias("discount_stddev"),
        F.sum(F.when(F.col("status") == "RETURNED", F.col("amount")).otherwise(0)).alias("returned_amount"),
        F.count(F.when(F.col("status") == "COMPLETED", 1)).alias("completed_rows"),
    )
    .filter(F.col("discount_stddev") < 0)
    .orderBy(F.col("completed_rows").desc_nulls_last())
)
