from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "right")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(~F.col("s.store_name").like("%Downto%") & F.col("s.city").isin(["Chiang Mai", "Bangkok"]))
    .select(
        F.col("ss.id"),
        F.col("ss.store_id"),
        F.col("ss.units"),
        F.col("p.brand"),
        F.col("p.category"),
        F.col("s.sqft"),
        F.col("s.store_name"),
    )
    .orderBy(F.col("units").desc())
)
