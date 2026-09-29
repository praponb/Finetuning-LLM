from pyspark.sql import functions as F

result = (
    df_orders
    .select("order_date")
    .intersect(
        df_store_sales
        .filter(F.col("units") != 13)
        .select(F.col("sale_date").alias("order_date"))
    )
    .orderBy(F.col("order_date").desc())
)
