from pyspark.sql import functions as F

filtered_customers = df_customers.filter(F.col("age").isNotNull())

summary = (
    filtered_customers
    .groupBy(
        F.when(F.col("city").isin(["Singapore", "Berlin"]), "group_a").otherwise("other").alias("city_group"),
    )
    .agg(F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"))
)

result = (
    summary
    .filter(F.col("avg_credit_limit") > 23)
    .select("city_group", "avg_credit_limit")
    .orderBy(F.col("avg_credit_limit").desc())
)
