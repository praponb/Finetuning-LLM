from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("brand").orderBy(F.col("stock_qty_minus_unit_price").asc_nulls_first())
w_part = Window.partitionBy("product_id")
unit_price_by_product_id = df_order_items.groupBy("product_id").agg(F.max("unit_price").alias("max_order_item_unit_price"))

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "product_id",
        "unit_price",
        F.greatest("quantity", "unit_price").alias("greatest_quantity_unit_price"),
        (F.col("quantity") + F.col("unit_price")).alias("line_total"),
    )
)

with_dims = (
    base
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "inner")
    .select(
        "e.id",
        "e.product_id",
        "e.unit_price",
        F.col("b.launch_date"),
        F.col("b.brand"),
        F.col("b.stock_qty"),
    )
)

remaining_keys = (
    with_dims
    .select("id")
    .subtract(
        df_order_items
        .filter(F.col("order_id") == 21)
        .select("id")
    )
)

verified = (
    with_dims
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter(F.col("brand").isNotNull())
    .withColumn("stock_qty_minus_unit_price", (F.col("stock_qty") - F.col("unit_price")))
)

enriched = (
    valid
    .filter(F.col("stock_qty") >= 114)
    .withColumn("stock_qty_minus_unit_price_dense_rank", F.dense_rank().over(w_grp))
    .withColumn("product_id_max_stock_qty", F.max("stock_qty").over(w_part))
)

pivoted = (
    enriched
    .groupBy("product_id")
    .pivot("brand", ["Globex", "Acme"])
    .agg(
        F.avg("stock_qty_minus_unit_price").alias("value_sum"),
        F.count(F.lit(1)).alias("cnt"),
    )
)

result = (
    pivoted
    .join(unit_price_by_product_id, on="product_id", how="left")
    .select(
        "product_id",
        "Globex_value_sum",
        "Globex_cnt",
        "Acme_value_sum",
        "Acme_cnt",
        F.col("max_order_item_unit_price"),
    )
    .orderBy(F.col("Globex_value_sum").desc())
)
