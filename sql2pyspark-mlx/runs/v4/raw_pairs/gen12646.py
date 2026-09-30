from pyspark.sql import functions as F

result = (
    df_products
    .select("id", "brand", F.col("price").alias("PRICE_VAL"), "cost")
    .unpivot(["id", "brand"], ["PRICE_VAL", "cost"], "kpi", "metric_value")
)
