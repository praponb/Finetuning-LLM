from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").desc(), "id")

filtered = (
    df_store_sales
    .filter(F.col("revenue") >= 3900)
    .withColumn("sale_date_plus_78d", F.date_add("sale_date", 78))
)

latest = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

with_dims = (
    latest
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.region"))
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("units") >= 39, "LARGE").otherwise("SMALL").alias("units_tier"),
    )
    .agg(
        F.max(F.when(F.col("region") == "North", F.col("revenue"))).alias("north_max_revenue"),
        F.corr("revenue", "units").alias("corr_revenue_units"),
        F.count(F.when(F.col("region") == "South", 1)).alias("south_count"),
        F.bit_or("units").alias("units_bit_or"),
        F.count("*").alias("cnt"),
        F.count_if(F.col("region") == "West").alias("west_events"),
    )
    .orderBy(F.col("cnt").desc_nulls_first())
)
