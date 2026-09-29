from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter((F.col("s.sqft") < 15668) & (F.col("ss.sale_date") <= "2023-05-01"))
    .select(
        F.col("ss.id"),
        F.col("ss.store_id"),
        F.col("ss.sale_date"),
        F.col("s.region"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
    )
    .orderBy("id")
    .limit(5)
)
