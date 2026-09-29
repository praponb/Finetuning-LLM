from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("units").desc(), "id")

joined = (
    df_store_sales
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "left")
    .select("e.*", F.col("s.opened_date"))
)

enriched = (
    joined
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.product_name"))
)

latest = (
    enriched
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

cleaned = (
    latest
    .filter(F.col("sale_date") > F.lit("2024-04-23").cast("date"))
    .select(
        "*",
        F.round((F.col("units") * 100), 2).alias("units_scaled"),
        F.ceil("units").alias("units_whole"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("units_whole") >= 61, "PREMIUM").otherwise("STANDARD").alias("units_whole_tier"),
    )
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.round(F.avg("units_scaled"), 2).alias("avg_units_scaled"),
        F.stddev_samp("revenue").alias("revenue_stddev"),
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.round(F.avg("units_whole"), 2).alias("avg_units_whole"),
    )
    .orderBy("total_revenue")
)
