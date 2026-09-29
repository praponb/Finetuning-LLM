from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())

latest_match = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date"))
)

result = (
    latest_match
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("region") == "East", F.col("sqft")).otherwise(0)).alias("east_sqft"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("region") == "Central", 1)), F.lit(0)), 2).alias("sqft_per_central"),
    )
)
