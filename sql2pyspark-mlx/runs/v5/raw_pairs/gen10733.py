from pyspark.sql import functions as F

joined = (
    df_orders
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.city") == "New York")
    .select("a.*", F.col("s.region"), F.col("s.opened_date"), F.col("s.sqft"))
)

result = (
    joined
    .select("id", "status", "channel", "amount", "discount", "sqft")
    .orderBy(F.col("discount").asc_nulls_first(), "status")
)
