from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("ss.product_id").isin([4, 5]))
    .select(
        F.col("ss.product_id"),
        F.col("p.cost"),
        F.col("p.product_name"),
        F.quarter(F.col("ss.sale_date")).alias("sale_date_quarter"),
    )
)
