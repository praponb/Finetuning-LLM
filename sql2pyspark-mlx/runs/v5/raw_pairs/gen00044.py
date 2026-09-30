from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.date_trunc("WEEK", "launch_date").alias("week_start"), "brand")
    .agg(
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.countDistinct("brand").alias("distinct_brands"),
        F.percentile_approx("cost", 0.95).alias("cost_p95"),
    )
    .filter(F.col("cost_p95") > 0)
)
