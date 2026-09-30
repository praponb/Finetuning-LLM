from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy(F.col("shipped_date").desc_nulls_first(), "id")

latest = (
    df_shipments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .groupBy("status", "carrier")
    .agg(
        F.count_if(F.col("carrier") == "FedEx").alias("fedex_events"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
    )
    .filter(F.col("fedex_events") > 1)
    .orderBy(F.col("fedex_events").desc())
)
