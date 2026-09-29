from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("s.sqft").between(7497, 12367))
    .groupBy(
        F.year(F.col("s.opened_date")).alias("opened_year"),
        F.date_trunc("MONTH", F.col("s.opened_date")).alias("opened_month"),
    )
    .agg(F.sum(F.col("ss.revenue")).alias("total_revenue"))
)
