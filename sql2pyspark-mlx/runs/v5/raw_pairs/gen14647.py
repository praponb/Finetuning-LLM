from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("credit_limit") >= 15500, "PREMIUM").otherwise("STANDARD").alias("credit_limit_tier"),
        "city",
    )
    .agg(
        F.percentile_approx("credit_limit", [0.25, 0.5, 0.75]).alias("credit_limit_quartiles"),
        F.count("*").alias("cnt"),
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.count("*").alias("row_count"),
    )
)
