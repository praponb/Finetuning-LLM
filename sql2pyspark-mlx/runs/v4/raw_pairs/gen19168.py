from pyspark.sql import functions as F

not_flagged = (
    df_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") >= F.col("s.opened_date")), "left_semi")
)

result = (
    not_flagged
    .select("id", "store_name", "city", "region", "opened_date")
    .orderBy(F.col("id").desc(), "opened_date")
)
