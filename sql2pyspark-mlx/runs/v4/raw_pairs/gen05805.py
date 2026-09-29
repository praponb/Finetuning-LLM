from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("discount") >= 0.41, "high").when(F.col("discount") >= 0.13, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(
        (F.max("discount") - F.min("discount")).alias("discount_range"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .filter(F.col("discount_range") > 30)
    .orderBy("discount_bucket", "discount_range")
    .limit(50)
)
