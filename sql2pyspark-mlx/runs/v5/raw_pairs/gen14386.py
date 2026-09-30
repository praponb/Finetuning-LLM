from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter((F.col("s.region") != "West") & F.lower(F.col("s.store_name")).like("%iam%"))
    .select(
        F.col("s.sqft"),
        F.col("ss.sale_date"),
        F.col("ss.revenue"),
        F.col("ss.product_id"),
    )
)
