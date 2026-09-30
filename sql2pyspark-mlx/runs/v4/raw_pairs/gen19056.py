from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("city").isin(["Berlin", "New York"]), "core").otherwise("other").alias("city_group"),
    )
    .agg(F.avg("credit_limit").alias("avg_credit_limit"))
    .filter(F.col("avg_credit_limit") >= 13700)
    .select("city_group", "avg_credit_limit")
)
