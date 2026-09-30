from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").isNull() & ~F.col("segment").isin(["Consumer", "SMB"]))
    .agg(
        F.max("signup_date").alias("latest_signup_date"),
        F.avg("age").alias("avg_age"),
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
    )
    .orderBy(F.col("avg_age").desc())
)
