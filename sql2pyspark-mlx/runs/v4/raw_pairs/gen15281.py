from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("last_restock")

deduped = (
    df_inventory
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

filtered = (
    deduped
    .filter(F.col("last_restock") >= F.lit("2023-03-12").cast("date"))
    .withColumn("last_restock_month_end", F.last_day("last_restock"))
)

ranked = (
    filtered
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    ranked
    .rollup("product_id")
    .agg(
        F.count("*").alias("event_count"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.grouping("product_id").alias("grouping_id"),
    )
    .orderBy(F.col("product_id").asc_nulls_first())
)
