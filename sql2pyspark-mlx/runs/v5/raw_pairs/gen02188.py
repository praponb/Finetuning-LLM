from pyspark.sql import functions as F

avg_cost = df_products.agg(F.avg("cost")).first()[0]

result = (
    df_products
    .filter(F.col("cost") > avg_cost)
    .select("id", "cost", "stock_qty")
)
