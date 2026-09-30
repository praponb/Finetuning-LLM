from pyspark.sql import functions as F

result = (
    df_products
    .select("cost")
    .intersect(
        df_order_items
        .filter(F.col("product_id").isNull())
        .select("unit_price")
    )
    .orderBy("cost")
)
