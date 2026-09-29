from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.filter(F.col("sale_date").between("2023-08-27", "2024-08-06")).alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left_semi")
    .select(
        F.col("s.id"),
        F.col("s.sqft"),
        F.col("s.opened_date"),
        F.col("s.store_name"),
    )
)
