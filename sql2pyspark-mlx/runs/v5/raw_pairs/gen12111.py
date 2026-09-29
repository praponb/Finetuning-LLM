from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.month("launch_date").alias("launch_month"))
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.sum(F.when(F.col("brand") == "Globex", 1).otherwise(0)).alias("globex_count"),
    )
    .filter(F.col("cost_range") >= 58)
    .orderBy(F.col("globex_count").desc())
)
