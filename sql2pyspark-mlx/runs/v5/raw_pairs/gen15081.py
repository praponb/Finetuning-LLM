from pyspark.sql import functions as F

result = (
    df_orders
    .select("order_date")
    .intersect(df_stores.select("opened_date"))
    .orderBy(F.col("order_date").desc())
)
