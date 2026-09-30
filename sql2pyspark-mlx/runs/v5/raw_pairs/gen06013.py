from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("country").isin(["TH", "SG"]), "core").otherwise("other").alias("country_group"),
    )
    .agg(
        F.min("age").alias("min_age"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.round(F.avg("age"), 2).alias("avg_age"),
    )
)
