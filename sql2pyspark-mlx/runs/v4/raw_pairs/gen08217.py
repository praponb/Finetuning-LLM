from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") != 4)
    .select("product_id")
    .unionByName(
        df_order_items
        .filter(F.col("order_id").isin([24, 35, 37, 39]))
        .select("product_id")
    )
    .orderBy("product_id")
)
