from pyspark.sql import functions as F

not_flagged = (
    df_stores
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.channel").isin(["store", "web"])), "left_anti")
)

result = (
    not_flagged
    .select("id", "store_name", "city", "region", "opened_date", "sqft")
    .orderBy(F.col("opened_date").asc_nulls_first())
)
