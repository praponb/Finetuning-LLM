from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("cost") >= 770, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
    )
)
