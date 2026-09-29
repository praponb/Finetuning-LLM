from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("week_start").orderBy(F.col("cost_per_kerry").desc())

result = (
    df_shipments
    .groupBy(
        F.date_trunc("WEEK", "delivered_date").alias("week_start"),
        F.when(F.col("cost") >= 76, "LARGE").otherwise("SMALL").alias("cost_tier"),
    )
    .agg(
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("carrier") == "Kerry", 1)), F.lit(0)), 2).alias("cost_per_kerry"),
        F.size(F.collect_set("carrier")).alias("num_carriers"),
        F.stddev_samp("cost").alias("cost_stddev"),
        F.stddev_samp("cost").alias("cost_stddev_2"),
        F.stddev_samp("cost").alias("cost_stddev_3"),
    )
    .withColumn("cost_per_kerry_rank", F.rank().over(w))
    .orderBy(F.col("cost_stddev").desc_nulls_last())
)
