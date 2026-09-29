from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("discount").desc_nulls_last(), "id")

with_dims = (
    df_order_items
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.amount"), F.col("b.discount"))
)

latest = (
    with_dims
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

matched_keys = (
    latest
    .select("order_id", "product_id")
    .subtract(
        df_order_items
        .filter(F.col("order_id").isin([5, 32, 36]))
        .select("order_id", "product_id")
    )
)

reconciled = (
    latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .withColumn("discount_tier", F.when(F.col("discount") >= 0.37, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["order_id", "discount_tier"], ["order_id"]],
        "order_id",
        "discount_tier",
    )
    .agg(
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.percentile_approx("discount", 0.9).alias("discount_p90"),
        F.avg("discount").alias("avg_discount"),
    )
    .orderBy(F.col("avg_discount").desc())
)
