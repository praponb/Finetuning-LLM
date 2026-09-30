from pyspark.sql import functions as F

result = (
    df_inventory
    .select("id", "quantity", "product_id")
    .orderBy(F.col("id").desc())
)
