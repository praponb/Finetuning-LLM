from pyspark.sql import functions as F

long_format = (
    df_products
    .select("id", "brand", "cost", "price")
    .unpivot(
        ids=["id", "brand"],
        values=["cost", "price"],
        variableColumnName="metric",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)

result = (
    long_format
    .groupBy(F.when(F.col("val") >= 68, "HIGH").otherwise("LOW").alias("val_tier"))
    .agg(
        F.round(F.avg("val"), 2).alias("avg_val"),
        F.round(F.sum("val") / F.nullif(F.count(F.when(F.col("metric") == "price", 1)), F.lit(0)), 2).alias("val_per_price"),
    )
    .filter(F.col("val_per_price") < 1000000)
    .orderBy(F.col("val_tier").desc())
)
