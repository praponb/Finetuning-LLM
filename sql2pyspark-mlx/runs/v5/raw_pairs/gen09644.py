from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").between(38, 44) & F.col("email").isNotNull())
    .groupBy("segment")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("segment"))).alias("segment_values"),
        F.avg("credit_limit").alias("avg_credit_limit"),
    )
    .orderBy("segment_values", "segment")
    .limit(10)
)
