from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("brand").isin(["Globex", "Initech"]) & F.col("supplier_id").isNull()) | (F.col("launch_date") >= "2025-04-01"))
    .withColumn("margin", F.round((F.col("cost") / F.col("price")), 2))
    .orderBy(F.col("stock_qty").desc(), F.col("price").desc())
)
