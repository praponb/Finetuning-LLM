from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy(F.col("line_total").desc(), "id")
w2 = Window.partitionBy("category")
w3 = Window.partitionBy("category").orderBy("quantity", "id")
w4 = Window.partitionBy("product_id", "category").orderBy("unit_price")
w5 = Window.orderBy(F.col("price").desc_nulls_last())
w6 = Window.partitionBy("order_id").orderBy(F.col("launch_date").desc(), "id")
w7 = Window.partitionBy("product_id", "category").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy(F.col("last_known_quantity").desc())
w9 = Window.partitionBy("order_id", "category")
w10 = Window.orderBy("launch_date", "id")

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("unit_price") < 380))
    .withColumn("line_total", (F.col("quantity") + F.col("unit_price")))
)

ranked = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

joined = (
    ranked
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.price"), F.col("p.launch_date"), F.col("p.category"))
)

scored = (
    joined
    .select(
        "*",
        F.round((F.col("quantity") / F.sum("quantity").over(w2)), 4).alias("quantity_share"),
        F.lead("unit_price", 1).over(w3).alias("next_unit_price"),
        F.rank().over(w4).alias("unit_price_rank"),
        F.rank().over(w5).alias("price_rank"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.row_number().over(w6).alias("event_seq"),
        F.last("quantity", ignorenulls=True).over(w7).alias("last_known_quantity"),
    )
)

result = (
    enriched
    .withColumn("last_known_quantity_dense_rank", F.dense_rank().over(w8))
    .withColumn("price_rank_share", F.round((F.col("price_rank") / F.sum("price_rank").over(w9)), 4))
    .withColumn("next_price", F.lead("price", 1).over(w10))
)
