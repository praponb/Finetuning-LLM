from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "carrier").orderBy("delivered_date", "id")

ranked = (
    df_shipments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    ranked
    .groupBy("carrier")
    .agg(
        F.count_if(F.col("carrier") == "DHL").alias("dhl_events"),
        F.percentile_approx("cost", 0.5).alias("cost_p50"),
        F.sum(F.when(F.col("status") == "DELIVERED", F.col("cost"))).alias("delivered_total_cost"),
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("carrier") == "Flash", 1)), F.lit(0)), 2).alias("cost_per_flash"),
    )
    .orderBy(F.col("cost_per_flash").asc_nulls_first())
)
