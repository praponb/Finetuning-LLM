from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select("id", "cost", "launch_date")
    .orderBy(F.col("launch_date").desc())
)
