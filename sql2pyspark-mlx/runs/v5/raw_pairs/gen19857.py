from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("max_item_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id")

unpacked = (
    df_carts
    .filter(F.col("customer_id").isin([8, 24]))
    .select(
        "id",
        "customer_id",
        "ship_to",
        F.coalesce(F.col("attributes").getItem("gift"), F.lit("unknown")).alias("gift_or_default"),
        F.array_max(F.transform("items", lambda item: item["price"])).alias("max_item_price"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "gift_or_default",
        F.sum("max_item_price").over(w).alias("cumulative_max_item_price"),
        F.round((F.col("max_item_price") / F.sum("max_item_price").over(w2)), 4).alias("max_item_price_share"),
    )
)
