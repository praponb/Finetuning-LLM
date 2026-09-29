from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("delivered_date").desc(), "id")

ranked = (
    df_shipments
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .groupBy(
        F.date_trunc("quarter", "delivered_date").alias("quarter_start"),
        "order_id",
    )
    .agg(
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.percentile_approx("cost", 0.9).alias("cost_p90"),
        F.stddev_samp("cost").alias("cost_stddev"),
        F.percentile_approx("cost", 0.5).alias("cost_p50"),
    )
    .filter(F.col("cost_p50") > 0)
)
