from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(~F.col("s.city").isin(["Bangkok", "London", "Tokyo"]))
    .groupBy(F.col("s.id"))
    .agg(F.count(F.col("p.id")).alias("product_count"))
    .filter(F.col("product_count") > 2)
    .orderBy(F.col("id").desc())
)
