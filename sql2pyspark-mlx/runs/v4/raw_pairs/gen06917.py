from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.amount") > 730)
    .groupBy(F.quarter(F.col("s.opened_date")).alias("opened_quarter"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("o.channel") == "store", 1).otherwise(0))) / F.count("*")), 1).alias("pct_store"),
    )
    .filter(F.col("pct_store") > 63)
    .orderBy("opened_quarter", F.col("pct_store").desc())
    .limit(50)
)
