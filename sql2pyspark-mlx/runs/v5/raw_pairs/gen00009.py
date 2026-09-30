from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("product_id").orderBy("quantity", "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc())
w_recent = Window.partitionBy("product_id").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("product_id")
w6 = Window.partitionBy("order_id").orderBy(F.col("cost").asc_nulls_last())
w7 = Window.partitionBy("product_brand")
w8 = Window.partitionBy("product_id").orderBy("launch_date", "id").rowsBetween(-6, -1)

scored = (
    df_order_items
    .select(
        "*",
        F.min("unit_price").over(w).alias("order_id_min_unit_price"),
        F.lead("quantity", 1, 0).over(w2).alias("next_quantity"),
        F.rank().over(w3).alias("unit_price_rank"),
        F.corr("unit_price", "quantity").over(w).alias("corr_unit_price_quantity"),
    )
)

joined = (
    scored
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id").eqNullSafe(F.col("p.id")), "inner")
    .filter(F.col("p.stock_qty") < 245)
    .select(
        "src.id",
        "src.order_id",
        "src.product_id",
        "src.unit_price",
        "src.order_id_min_unit_price",
        "src.next_quantity",
        "src.unit_price_rank",
        "src.corr_unit_price_quantity",
        F.col("p.brand"),
        F.col("p.cost"),
        F.col("p.launch_date"),
    )
)

with_windows = (
    joined
    .select(
        "*",
        F.sum("order_id_min_unit_price").over(w_recent).alias("cumulative_order_id_min_unit_price"),
        F.min("next_quantity").over(w_grp).alias("product_id_min_next_quantity"),
        F.cume_dist().over(w6).alias("cost_cume_dist"),
    )
)

joined_order_items = (
    with_windows
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "src.id",
        "src.product_id",
        "src.unit_price",
        "src.order_id_min_unit_price",
        "src.unit_price_rank",
        "src.cost",
        "src.launch_date",
        "src.product_id_min_next_quantity",
        F.col("b.brand").alias("product_brand"),
        F.col("b.cost").alias("product_cost"),
    )
)

result = (
    joined_order_items
    .select(
        "id",
        "product_id",
        "unit_price_rank",
        "cost",
        "product_cost",
        F.round((F.col("unit_price") / F.sum("unit_price").over(w7)), 4).alias("unit_price_share"),
        F.round((F.col("unit_price") / F.sum("unit_price").over(w7)), 4).alias("unit_price_share_2"),
        F.count("product_id_min_next_quantity").over(w_grp).alias("product_id_count_product_id_min_next_quantity"),
        F.avg("product_id_min_next_quantity").over(w8).alias("product_id_min_next_quantity_avg_6_prior"),
    )
)
