from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("discount").asc_nulls_first())

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "unit_price",
        F.least("unit_price", "quantity").alias("least_unit_price_quantity"),
    )
)

kept_keys = (
    base
    .select("order_id")
    .subtract(
        df_order_items
        .filter(F.col("order_id") == 16)
        .select("order_id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("least_unit_price_quantity") < 80)
    .withColumn("unit_price_per_least_unit_price_quantity", (F.col("unit_price") / F.nullif("least_unit_price_quantity", F.lit(0))))
)

with_dims = (
    valid
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .filter(F.col("p.shipping_fee") <= 10)
    .select("a.*", F.col("p.amount"), F.col("p.discount"), F.col("p.order_date"))
)

metrics = with_dims.withColumn("discount_percent_rank", F.percent_rank().over(w))

remaining_keys = (
    metrics
    .select("id", "order_id")
    .subtract(
        df_order_items
        .filter(F.col("product_id").isNotNull())
        .select("id", "order_id")
    )
)

verified = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .rollup(F.col("o.status").alias("status"))
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("a.unit_price_per_least_unit_price_quantity")).alias("total_unit_price_per_least_unit_price_quantity"),
    )
)
