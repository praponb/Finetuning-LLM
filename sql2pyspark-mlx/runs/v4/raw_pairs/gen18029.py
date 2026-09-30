from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNull())
    .select("id", "supplier_id", "launch_date", "price")
)
