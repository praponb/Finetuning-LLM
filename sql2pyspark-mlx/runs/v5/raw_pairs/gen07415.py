from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.brand").isin(["Umbrella", "Initech", "Globex"]))
    .groupBy(F.quarter(F.col("s.opened_date")).alias("opened_quarter"))
    .agg(
        F.round(F.stddev(F.col("ss.revenue")), 2).alias("stddev_revenue"),
        F.max(F.col("ss.units")).alias("max_units"),
    )
    .orderBy("stddev_revenue")
)
