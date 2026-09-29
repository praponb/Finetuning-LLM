from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "brand").orderBy("cost")

latest = (
    df_products
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    latest
    .rollup(
        F.when(F.col("cost") >= 750, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
        F.date_trunc("WEEK", "launch_date").alias("week_start"),
    )
    .agg(
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.percentile_approx("price", 0.9).alias("price_p90"),
        F.bit_and("stock_qty").alias("stock_qty_bit_and"),
    )
)
