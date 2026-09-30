from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.filter(~F.col("brand").isin(["Umbrella", "Acme"])).alias("p"), F.col("oi.product_id") == F.col("p.id"), "left_semi")
    .filter(F.col("oi.unit_price").between(120, 310))
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("oi.product_id"),
        F.col("oi.quantity"),
    )
)
