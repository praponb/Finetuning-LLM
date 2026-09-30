from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") != 340)
    .groupBy("event_type")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("page") == "/product", 1).otherwise(0))) / F.count("*")), 1).alias("pct_product"),
    )
)
