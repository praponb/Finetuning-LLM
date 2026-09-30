from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") >= 350)
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("launch_month"), "brand")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("brand") == "Umbrella", 1).otherwise(0))) / F.count("*")), 1).alias("pct_umbrella"),
    )
)
