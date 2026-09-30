from pyspark.sql import functions as F

result = (
    df_shipments
    .cube(
        "carrier",
        F.when(F.col("cost") >= 105, "high").when(F.col("cost") >= 39, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .orderBy("cost_bucket")
)
