from pyspark.sql import functions as F

long_format = (
    df_products
    .select(
        "id",
        "category",
        F.col("price").alias("PRICE"),
        F.col("cost").alias("COST_VAL"),
    )
    .unpivot(
        ids=["id", "category"],
        values=["PRICE", "COST_VAL"],
        variableColumnName="measure",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)

result = (
    long_format
    .groupBy(
        F.when(F.col("val") >= 84, "LARGE").otherwise("SMALL").alias("val_tier"),
        "measure",
    )
    .agg(
        F.percentile_approx("val", [0.25, 0.5, 0.75]).alias("val_quartiles"),
        F.sum(F.when(F.col("measure") == "COST_VAL", F.col("val")).otherwise(0)).alias("cost_val_val"),
        F.count_if(F.col("measure") == "PRICE").alias("price_events"),
        F.percentile_approx("val", 0.95).alias("val_p95"),
        F.round(F.avg("val"), 2).alias("avg_val"),
    )
    .orderBy(F.col("cost_val_val").asc_nulls_last())
)
