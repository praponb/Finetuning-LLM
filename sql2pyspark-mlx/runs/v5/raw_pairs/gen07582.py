from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity_filled").desc())
w2 = Window.partitionBy("order_id", "product_id").orderBy(F.col("quantity").desc())
w_grp = Window.orderBy(F.col("unit_price").desc_nulls_first())

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("unit_price") > 150))
    .withColumn("quantity_filled", F.coalesce("quantity", F.lit(0)))
)

kept_keys = (
    valid
    .select("id", "order_id")
    .subtract(
        df_order_items
        .filter(F.col("order_id").isin([1, 16, 24, 28]))
        .select("id", "order_id")
    )
)

verified = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

remaining_keys = (
    verified
    .select("product_id", "order_id")
    .exceptAll(
        df_order_items
        .filter(F.col("order_id") == 35)
        .select("product_id", "order_id")
    )
)

kept_rows = (
    verified
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .select(
        "order_id",
        "product_id",
        "unit_price",
        F.percent_rank().over(w).alias("quantity_filled_percent_rank"),
        F.percent_rank().over(w2).alias("quantity_percent_rank"),
        F.percent_rank().over(w_grp).alias("unit_price_percent_rank"),
    )
)

cleaned = (
    scored
    .filter(F.col("unit_price_percent_rank").between(0.61, 0.73))
    .withColumn("unit_price_percent_rank_minus_quantity_percent_rank", (F.col("unit_price_percent_rank") - F.col("quantity_percent_rank")))
)

result = (
    cleaned
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .rollup(F.col("p.category").alias("category"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("a.quantity_filled_percent_rank")), 2).alias("total_quantity_filled_percent_rank"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("category").desc_nulls_last())
)
