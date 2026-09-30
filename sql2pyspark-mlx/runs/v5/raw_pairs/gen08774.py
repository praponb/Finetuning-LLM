from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.col("quantity") >= 248, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
    )
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
    .filter(F.col("avg_quantity") > 0)
    .orderBy("quantity_tier")
)
