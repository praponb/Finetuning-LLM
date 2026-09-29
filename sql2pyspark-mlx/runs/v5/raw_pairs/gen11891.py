from pyspark.sql import functions as F

cleaned = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & F.col("supplier_id").isin([2, 21, 28, 39]))
    .withColumn("is_clearance", F.array_contains("tags", "clearance"))
)

result = (
    cleaned
    .groupBy("category")
    .pivot("brand", ["Initech", "Acme"])
    .agg(F.avg("price"))
)
