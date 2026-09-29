from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("cost") < 700) & ((F.col("cost") >= 280) | (F.col("brand") == "Initech")))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("brand") == "Globex", 1).otherwise(0))) / F.count("*")), 1).alias("pct_globex"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
)
