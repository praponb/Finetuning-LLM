from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(~F.col("s.region").isin(["South", "Central", "North"]))
    .groupBy(F.col("s.city"))
    .agg(
        F.sum(F.col("ss.units")).alias("total_units"),
        F.min(F.col("ss.sale_date")).alias("min_sale_date"),
    )
    .orderBy("total_units")
)
