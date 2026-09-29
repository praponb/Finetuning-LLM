from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.filter(F.col("amount").between(580, 850)).alias("o"), F.col("s.id") == F.col("o.store_id"), "left_semi")
    .select(F.col("s.id"), F.col("s.city"), F.col("s.opened_date"))
)
