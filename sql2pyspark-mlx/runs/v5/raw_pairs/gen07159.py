from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("cost", "id")
w2 = Window.orderBy(F.col("cost").desc_nulls_last())
w3 = Window.partitionBy("product_id").orderBy("quantity_str", "id")
w4 = Window.partitionBy("product_id").orderBy(F.col("quantity_str").desc(), "id")

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("unit_price") > 270))
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        F.col("quantity").cast("double").alias("quantity_str"),
    )
)

with_dims = (
    cleaned
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "src.id",
        "src.product_id",
        "src.quantity_str",
        F.col("d.product_name"),
        F.col("d.brand"),
        F.col("d.launch_date"),
    )
)

enriched = (
    with_dims
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select("e.id", "e.product_id", "e.quantity_str", F.col("p.cost"))
)

scored = (
    enriched
    .select(
        "id",
        "product_id",
        "quantity_str",
        F.ntile(4).over(w).alias("cost_bucket"),
        F.dense_rank().over(w2).alias("cost_dense_rank"),
        F.lag("cost", 2).over(w3).alias("prev_cost"),
    )
)

ranked = (
    scored
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .select(
        "id",
        "product_id",
        "quantity_str",
        "cost_bucket",
        "cost_dense_rank",
        "prev_cost",
        "row_num",
    )
    .orderBy("row_num")
)
