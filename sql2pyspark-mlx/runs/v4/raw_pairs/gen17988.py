from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.filter(~F.col("city").isin(["New York", "Chiang Mai", "Berlin"])).alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left_semi")
    .filter(F.col("p.price") >= 310)
    .select(F.col("p.id"), F.col("p.launch_date"), F.col("p.price"))
)
