from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.store_name").like("%Expres%"))
    .select(
        F.col("ss.store_id"),
        F.col("ss.sale_date"),
        F.col("s.opened_date"),
        F.col("s.region"),
        F.col("s.store_name"),
        F.datediff(F.current_date(), F.col("ss.sale_date")).alias("days_since_sale_date"),
    )
    .orderBy("opened_date")
)
