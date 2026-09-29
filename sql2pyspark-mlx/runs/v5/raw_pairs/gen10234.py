from pyspark.sql import functions as F

result = (
    df_orders
    .select("id", "order_date", F.coalesce("amount", F.lit(0)).alias("amount_filled"))
)
