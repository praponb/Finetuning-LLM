from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.filter(F.col("opened_date").between("2023-08-01", "2024-07-01")).alias("s"), F.col("ss.store_id") == F.col("s.id"), "left_anti")
    .select(F.col("ss.id"), F.col("ss.product_id"), F.col("ss.units"))
    .orderBy(F.col("product_id").desc(), "id")
)
