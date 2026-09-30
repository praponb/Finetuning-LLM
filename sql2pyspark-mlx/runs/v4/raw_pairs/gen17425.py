from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.when(F.col("cost") >= 86, "LARGE").otherwise("SMALL").alias("cost_tier"))
    .agg(
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("carrier") == "FedEx", 1)), F.lit(0)), 2).alias("cost_per_fedex"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
    )
    .orderBy("cost_per_fedex")
)
