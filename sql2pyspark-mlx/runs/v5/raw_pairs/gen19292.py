from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .agg(
        F.countDistinct("credit_limit").alias("unique_countrys"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
    .filter(F.col("unique_countrys") < 3)
)
