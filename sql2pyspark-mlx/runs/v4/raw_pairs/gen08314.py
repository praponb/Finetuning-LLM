from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter((F.col("s.sqft") <= 15720) & F.col("s.opened_date").between("2023-04-01", "2023-08-01"))
    .select(F.col("s.sqft"), F.col("s.region"), F.col("ss.product_id"))
)
