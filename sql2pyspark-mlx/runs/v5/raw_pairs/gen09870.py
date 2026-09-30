from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id")

latest = (
    df_inventory
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .groupBy(
        F.date_trunc("year", "last_restock").alias("year_start"),
        F.date_trunc("month", "last_restock").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.percentile_approx("quantity", 0.9).alias("quantity_p90"),
    )
    .orderBy(F.col("quantity_stddev").asc_nulls_first())
)
