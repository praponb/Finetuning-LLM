from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.stock_qty").between(186, 369) | F.col("p.brand").isNotNull())
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("ss.units"),
        F.col("ss.sale_date"),
        F.col("p.brand"),
        F.col("p.price"),
        F.col("p.cost"),
    )
)
