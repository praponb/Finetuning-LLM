from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.col("supplier_id").isin([3, 17])).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_anti")
    .filter(~F.col("s.city").isin(["New York", "Bangkok", "Chiang Mai"]))
    .select(F.col("s.id"), F.col("s.supplier_name"))
)
