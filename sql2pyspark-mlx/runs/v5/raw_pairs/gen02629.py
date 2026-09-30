from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_type") == "click") & (F.col("page") == "/search"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("page") == "/product", 1).otherwise(0))) / F.count("*")), 1).alias("pct_product"),
    )
    .orderBy(F.col("pct_product").desc())
)
