from pyspark.sql import functions as F

metrics_long = (
    df_products
    .select("id", "supplier_id", "price", "cost")
    .unpivot(
        ids=["id", "supplier_id"],
        values=["price", "cost"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)

result = (
    metrics_long
    .groupBy(
        F.when(F.col("value") >= 64, "LARGE").otherwise("SMALL").alias("value_tier"),
    )
    .agg(
        F.size(F.collect_set("metric")).alias("num_metrics"),
        F.percentile_approx("value", [0.25, 0.5, 0.75]).alias("value_quartiles"),
    )
    .orderBy(F.col("num_metrics").desc())
)
