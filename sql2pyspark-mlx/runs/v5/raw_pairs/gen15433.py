from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter((F.col("s.region") != "East") & F.col("ss.product_id").isin([1, 2, 3, 7]))
    .select(
        F.col("s.region"),
        F.col("s.city"),
        F.col("s.sqft"),
        F.col("ss.revenue"),
        F.col("ss.sale_date"),
    )
)
