from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "right")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.stock_qty") >= 298)
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("s.region"),
        F.col("s.opened_date"),
        F.col("p.cost"),
    )
    .orderBy("cost", "region")
)
