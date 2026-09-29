from pyspark.sql import functions as F

kept = (
    df_stores
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.order_date") <= F.col("s.opened_date")), "left_anti")
)

result = (
    kept
    .select("id", "store_name", "city", "opened_date", "sqft")
    .orderBy(F.col("city").desc(), "id")
)
