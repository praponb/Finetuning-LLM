from pyspark.sql import functions as F

min_discount = df_orders.filter(F.col("amount") > 1470).agg(F.min("discount")).first()[0]

result = (
    df_orders
    .filter(F.col("discount") == min_discount)
    .select("id", "discount")
)
