from pyspark.sql import functions as F

result = (
    df_orders
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(((100 * F.sum(F.when(F.col("channel") == "partner", 1).otherwise(0))) / F.count("*")), 1).alias("pct_partner"),
    )
)
