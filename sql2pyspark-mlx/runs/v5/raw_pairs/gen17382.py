from pyspark.sql import functions as F

result = (
    df_payments
    .select("amount")
    .intersect(df_products.select("cost"))
)
