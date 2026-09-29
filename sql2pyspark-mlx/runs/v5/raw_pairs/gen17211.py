from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.max("opened_date").alias("latest_opened_date"),
        F.round(((100 * F.sum(F.when(F.col("city") == "Singapore", 1).otherwise(0))) / F.count("*")), 1).alias("pct_singapore"),
        F.countDistinct("sqft").alias("unique_citys"),
    )
    .filter(F.col("unique_citys") >= 1)
    .drop("unique_citys")
)
