from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter((F.col("ss.sale_date") > "2024-02-01") & (F.col("p.price") >= 1140))
    .select(
        F.col("ss.revenue"),
        F.col("s.region"),
        F.col("s.opened_date"),
        F.col("p.cost"),
        F.round((F.col("p.cost") * 0.9), 2).alias("adjusted_cost"),
    )
    .orderBy("adjusted_cost")
)
