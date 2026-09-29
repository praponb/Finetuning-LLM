from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("amount").desc(), "id")

valid = (
    df_order_items
    .filter(F.col("order_id").isin([3, 16, 19]))
    .withColumn("adjusted_unit_price", F.round((F.col("unit_price") * 1.15), 2))
)

joined = (
    valid
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.product_id",
        "src.quantity",
        F.col("o.amount"),
        F.col("o.channel"),
    )
)

base = (
    joined
    .filter(F.col("channel").isNotNull() & (F.col("order_id") == 16))
    .withColumn("amount_whole", F.floor("amount"))
)

latest = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

cleaned = (
    latest
    .filter(F.col("channel").isNotNull() & (F.col("quantity") >= 7))
    .select(
        "id",
        "order_id",
        "product_id",
        "amount",
        "amount_whole",
        "dedup_rank",
        (F.col("amount_whole") - F.col("amount")).alias("amount_whole_minus_amount"),
        F.least("quantity", "dedup_rank").alias("least_quantity_dedup_rank"),
    )
)

result = (
    cleaned
    .alias("src")
    .join(df_orders.alias("dim"), F.col("src.order_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["channel", "status"], ["channel"], []],
        F.col("dim.channel").alias("channel"),
        F.col("dim.status").alias("status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("src.least_quantity_dedup_rank")), 2).alias("total_least_quantity_dedup_rank"),
        F.sum(F.col("src.amount_whole_minus_amount")).alias("total_amount_whole_minus_amount"),
        F.grouping_id().alias("grouping_level"),
    )
)
