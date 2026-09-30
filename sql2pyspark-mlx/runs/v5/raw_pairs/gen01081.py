from pyspark.sql import functions as F

metrics_long = (
    df_products
    .select(
        "id",
        "brand",
        F.col("cost").alias("COST_VAL"),
        F.col("price").alias("PRICE"),
    )
    .unpivot(
        ids=["id", "brand"],
        values=["COST_VAL", "PRICE"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)

result = (
    metrics_long
    .cube(
        F.when(F.col("value") >= 70, "HIGH").otherwise("LOW").alias("value_tier"),
        "brand",
    )
    .agg(
        F.percentile_approx("value", [0.25, 0.5, 0.75]).alias("value_quartiles"),
        F.percentile_approx("value", [0.25, 0.5, 0.75]).alias("value_quartiles_2"),
        F.percentile_approx("value", 0.9).alias("value_p90"),
        F.round(F.sum("value") / F.nullif(F.count(F.when(F.col("brand") == "Stark", 1)), F.lit(0)), 2).alias("value_per_stark"),
        F.avg(F.when(F.col("measure") == "COST_VAL", F.col("value"))).alias("cost_val_avg_value"),
        F.round(F.sum("value") / F.nullif(F.count(F.when(F.col("brand") == "Initech", 1)), F.lit(0)), 2).alias("value_per_initech"),
    )
    .orderBy("value_p90")
)
