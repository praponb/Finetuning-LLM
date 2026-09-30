from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel") == "web")
    .select("order_date")
    .subtract(df_stores.select("opened_date"))
    .orderBy(F.col("order_date").desc())
)
