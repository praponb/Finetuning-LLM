from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active", "country")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("segment"))).alias("segment_values"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
        F.avg("credit_limit").alias("avg_credit_limit"),
    )
    .filter(F.col("avg_credit_limit") > 11900)
)
