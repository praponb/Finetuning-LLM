from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "left")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("ss.sale_date"),
        F.col("s.opened_date"),
        F.col("s.city"),
        F.col("p.launch_date"),
        F.date_trunc("week", F.col("s.opened_date")).alias("opened_date_period"),
    )
    .orderBy(F.col("product_id").desc(), "id")
    .limit(3)
)
