from pyspark.sql import functions as F

long_format = (
    df_products
    .select("id", "supplier_id", "price", "cost")
    .unpivot(
        ids=["id", "supplier_id"],
        values=["price", "cost"],
        variableColumnName="kpi",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)

result = (
    long_format
    .groupBy(F.when(F.col("value") >= 75, "HIGH").otherwise("LOW").alias("value_tier"))
    .agg(
        F.max(F.when(F.col("kpi") == "cost", F.col("value"))).alias("cost_max_value"),
        F.count("*").alias("cnt"),
        F.percentile_approx("value", [0.25, 0.5, 0.75]).alias("value_quartiles"),
    )
)
