from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") > 500)
    .select("id", "amount")
    .filter(F.col("id") < 20)
)
