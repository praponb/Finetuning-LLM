from pyspark.sql import functions as F

unpivoted = (
    df_products
    .select("id", "supplier_id", "cost", "price")
    .unpivot(["id", "supplier_id"], ["cost", "price"], "kpi", "metric_value")
    .filter(F.col("metric_value").isNotNull())
)

result = (
    unpivoted
    .groupBy(
        F.when(F.col("metric_value") >= 77, "PREMIUM").otherwise("STANDARD").alias("metric_value_tier"),
    )
    .agg(
        F.percentile_approx("metric_value", 0.95).alias("metric_value_p95"),
        F.sum("metric_value").alias("total_metric_value"),
        F.percentile_approx("metric_value", [0.25, 0.5, 0.75]).alias("metric_value_quartiles"),
        F.sum("metric_value").alias("total_metric_value_2"),
        F.sum("metric_value").alias("total_metric_value_3"),
    )
    .orderBy(F.col("total_metric_value_2").desc())
)
