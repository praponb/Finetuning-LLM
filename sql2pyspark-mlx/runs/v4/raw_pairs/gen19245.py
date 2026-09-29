from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .select("order_id", "quantity")
    .unionByName(
        df_order_items
        .filter(F.col("unit_price") > 200)
        .select("order_id", "quantity")
    )
)
