from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w_part = Window.partitionBy("customer_id").orderBy("amount", "id").rowsBetween(-6, Window.currentRow)

base = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-08-23").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("channel_status_key", F.concat_ws("/", "channel", "status"))
)

with_last = (
    base
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.amount",
        F.col("c.unit_price").alias("order_item_unit_price"),
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

scored = (
    with_last
    .withColumn("last7_min_amount", F.min("amount").over(w_part))
    .withColumn("last7_sum_amount", F.sum("amount").over(w_part))
    .withColumn("last7_count_amount", F.count("amount").over(w_part))
)

matched_keys = (
    scored
    .select("id", "customer_id")
    .exceptAll(
        df_orders
        .filter(F.col("channel").isin(["web", "mobile"]))
        .select("id", "customer_id")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("customer_id", "amount", "order_item_quantity", "last7_count_amount")
)
