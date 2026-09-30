from pyspark.sql import functions as F

not_flagged = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.stock_qty") >= 363), "left_anti")
)

result = not_flagged.select("id", "supplier_name", "country", "city", "rating")
