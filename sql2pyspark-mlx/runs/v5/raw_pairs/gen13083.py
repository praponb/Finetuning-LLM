from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())

latest_match = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.revenue"), F.col("c.units"))
)

result = (
    latest_match
    .groupBy(
        F.when(F.col("units") >= 44, "PREMIUM").otherwise("STANDARD").alias("units_tier"),
    )
    .agg(
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"),
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("city") == "Singapore", 1)), F.lit(0)), 2).alias("sqft_per_singapore"),
        F.sum(F.when(F.col("city") == "Berlin", F.col("units"))).alias("berlin_total_units"),
    )
)
