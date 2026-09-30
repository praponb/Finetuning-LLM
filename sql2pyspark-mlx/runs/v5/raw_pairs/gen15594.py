from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "left")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter((F.col("p.launch_date") < "2025-10-01") & F.col("s.opened_date").between("2023-03-27", "2023-09-09"))
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("s.region"),
        F.col("s.city"),
        F.col("p.category"),
        F.date_trunc("MONTH", F.col("p.launch_date")).alias("launch_date_period"),
    )
)
