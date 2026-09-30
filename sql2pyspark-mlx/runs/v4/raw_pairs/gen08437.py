from pyspark.sql import functions as F

result = (
    df_products
    .select("id", "brand", "cost", "price")
    .unpivot(["id", "brand"], ["cost", "price"], "metric", "value")
    .filter(F.col("value").isNotNull())
)
