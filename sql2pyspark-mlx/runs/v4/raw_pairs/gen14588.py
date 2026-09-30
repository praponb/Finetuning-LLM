from pyspark.sql import functions as F

base = df_stores.filter(F.col("city").isin(["London", "New York", "Bangkok"]))

agg = (
    base
    .groupBy("region", F.year("opened_date").alias("opened_year"))
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.round(((100 * F.sum(F.when(F.col("region") == "West", 1).otherwise(0))) / F.count("*")), 1).alias("pct_west"),
    )
)

result = (
    agg
    .filter(F.col("pct_west") >= 71)
    .select("region", "opened_year", "min_opened_date", "pct_west")
    .orderBy("region", "pct_west")
)
