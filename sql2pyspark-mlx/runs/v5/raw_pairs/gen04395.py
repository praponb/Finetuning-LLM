from pyspark.sql import functions as F

not_flagged = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.lower(F.trim(F.col("c.product_name"))) == "laptop pro 14"), "left_semi")
)

result = not_flagged.select("id", "supplier_name", "rating")
