from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("cost") >= 240) & F.col("brand").isin(["Initech", "Acme"]) & F.col("supplier_id").isNotNull())
    .orderBy("launch_date")
)
