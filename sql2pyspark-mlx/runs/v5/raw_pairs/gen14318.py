from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") > 340)
    .select("quantity")
    .unionByName(
        df_order_items
        .filter(F.col("product_id").isin([20, 21, 25, 29]))
        .select("quantity")
    )
    .distinct()
)
