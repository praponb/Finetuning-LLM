from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.filter(~F.col("store_name").like("R%")).alias("s"), F.col("ss.store_id") == F.col("s.id"), "left_semi")
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("ss.product_id"),
        F.col("ss.store_id"),
    )
)
