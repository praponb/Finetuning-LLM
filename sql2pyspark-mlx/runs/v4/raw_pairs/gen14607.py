from pyspark.sql import functions as F

result = (
    df_orders
    .select("shipping_fee")
    .intersect(
        df_products
        .filter(F.col("brand").isin(["Globex", "Stark"]))
        .select("price")
    )
    .orderBy("shipping_fee")
)
