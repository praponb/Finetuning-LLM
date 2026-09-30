from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city", F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        F.count("*").alias("total_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
    )
    .filter(F.col("total_count") < 5)
    .orderBy("region_values", "opened_month")
)
