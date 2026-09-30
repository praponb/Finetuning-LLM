from pyspark.sql import functions as F

result = (
    df_order_items
    .select("unit_price")
    .unionByName(
        df_products
        .filter(F.month("launch_date").isin([1, 3, 9]))
        .select(F.col("cost").alias("unit_price"))
    )
    .orderBy("unit_price")
)
