from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "DE")
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.count("credit_limit").alias("credit_limit_count"),
    )
    .orderBy(F.col("credit_limit_count").desc())
)
