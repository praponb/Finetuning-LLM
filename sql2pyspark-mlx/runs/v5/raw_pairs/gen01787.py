from pyspark.sql import functions as F

unpivoted = (
    df_products
    .select("id", "supplier_id", F.col("cost").alias("COST"), "price")
    .unpivot(
        ids=["id", "supplier_id"],
        values=["COST", "price"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
)

result = (
    unpivoted
    .groupBy(
        F.when(F.col("metric_value") >= 88, "HIGH").otherwise("LOW").alias("metric_value_tier"),
    )
    .agg(
        F.count_if(F.col("measure") == "price").alias("price_events"),
        F.round(F.sum("metric_value") / F.nullif(F.count(F.when(F.col("measure") == "COST", 1)), F.lit(0)), 2).alias("metric_value_per_cost"),
        F.round(F.sum("metric_value") / F.nullif(F.count(F.when(F.col("measure") == "COST", 1)), F.lit(0)), 2).alias("metric_value_per_cost_2"),
        F.percentile_approx("metric_value", [0.25, 0.5, 0.75]).alias("metric_value_quartiles"),
    )
    .orderBy("price_events")
)
