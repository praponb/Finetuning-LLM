from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("cost").asc_nulls_first(), "id")

latest = (
    df_products
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .groupBy(F.date_trunc("YEAR", "launch_date").alias("year_start"))
    .agg(
        F.avg("price").alias("avg_price"),
        F.stddev_samp("price").alias("price_stddev"),
        F.count(F.when(F.col("brand") == "Umbrella", 1)).alias("umbrella_count"),
        F.round(F.sum("stock_qty") / F.nullif(F.count(F.when(F.col("brand") == "Umbrella", 1)), F.lit(0)), 2).alias("stock_qty_per_umbrella"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
    )
    .orderBy(F.col("year_start").desc())
)
