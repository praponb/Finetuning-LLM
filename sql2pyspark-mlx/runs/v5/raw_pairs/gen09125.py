from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter((F.col("s.country") == "US") & (F.col("p.category") == "Books"))
    .select(
        F.col("p.id"),
        F.col("p.cost"),
        F.col("s.city"),
        F.when(F.col("p.stock_qty") >= 309, "High").when(F.col("p.stock_qty") >= 112, "Medium").otherwise("Low").alias("stock_qty_band"),
    )
)
