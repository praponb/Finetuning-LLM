from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("product_id").orderBy("revenue", "id")
w_hist = Window.partitionBy("product_id").orderBy("sale_date", "id")
w4 = Window.partitionBy("store_id").orderBy(F.col("avg_prev_units").desc())

enriched = (
    df_store_sales
    .withColumn("last_known_revenue", F.last("revenue", ignorenulls=True).over(w))
    .withColumn("next_units", F.lead("units", 2).over(w_recent))
    .withColumn("prev_units", F.lag("units", 1).over(w_hist))
)

result = (
    enriched
    .groupBy(
        "store_id",
        F.when(F.col("last_known_revenue") >= 3300, "HIGH").otherwise("LOW").alias("last_known_revenue_tier"),
    )
    .agg(
        F.round(F.avg("prev_units"), 2).alias("avg_prev_units"),
        F.bit_and("units").alias("units_bit_and"),
        F.stddev_samp("units").alias("units_stddev"),
    )
    .filter(F.col("units_stddev") < 0)
    .withColumn("avg_prev_units_percent_rank", F.percent_rank().over(w4))
    .orderBy("avg_prev_units")
)
