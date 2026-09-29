from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_first())
unit_price_by_product_id = df_order_items.groupBy("product_id").agg(F.sum("unit_price").alias("sum_order_item_unit_price"))

filtered = (
    df_inventory
    .filter(F.col("quantity") > 72)
    .select(
        "id",
        "product_id",
        "quantity",
        F.dayofmonth("last_restock").alias("last_restock_day"),
    )
)

ranked = (
    filtered
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "product_id", "last_restock_day")
)

filtered_inventory = (
    ranked
    .filter(F.col("last_restock_day") != 76)
    .select(
        "*",
        F.round((F.col("last_restock_day") * 1.1), 2).alias("last_restock_day_scaled"),
    )
)

joined = (
    filtered_inventory
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.category").isin(["Home", "Fashion"]))
    .select("e.*", F.col("p.cost"), F.col("p.category"))
)

result = (
    joined
    .join(unit_price_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "last_restock_day",
        "last_restock_day_scaled",
        F.col("sum_order_item_unit_price"),
    )
    .orderBy("product_id", "id")
)
