from pyspark.sql import functions as F

unpivoted = (
    df_products
    .select("id", "category", "price", "cost")
    .unpivot(
        ids=["id", "category"],
        values=["price", "cost"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
)

result = (
    unpivoted
    .groupBy(
        F.when(F.col("metric_value") >= 86, "LARGE").otherwise("SMALL").alias("metric_value_tier"),
    )
    .agg(
        F.avg(F.when(F.col("metric_name") == "price", F.col("metric_value"))).alias("price_avg_metric_value"),
        F.round(F.sum("metric_value") / F.nullif(F.count(F.when(F.col("metric_name") == "cost", 1)), F.lit(0)), 2).alias("metric_value_per_cost"),
        F.count("*").alias("cnt"),
        F.count_if(F.col("category") == "Electronics").alias("electronics_events"),
    )
    .orderBy(F.col("metric_value_tier").asc_nulls_last())
)
