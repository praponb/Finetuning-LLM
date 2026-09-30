from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter(F.col("s.city").isin(["Tokyo", "Singapore", "Bangkok"]))
    .select(
        F.col("ss.sale_date"),
        F.col("ss.store_id"),
        F.col("s.sqft"),
        F.col("p.product_name"),
        F.col("p.cost"),
        (F.col("p.stock_qty") - F.col("p.price")).alias("stock_qty_minus_price"),
    )
    .orderBy(F.col("sqft").desc())
)
