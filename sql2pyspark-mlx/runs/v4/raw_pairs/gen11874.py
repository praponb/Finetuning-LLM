from pyspark.sql import functions as F

result = (
    df_customers
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("segment"))).alias("segment_values"),
        F.sum(F.when(F.col("country") == "TH", 1).otherwise(0)).alias("th_count"),
        F.countDistinct("age").alias("unique_countrys"),
    )
)
