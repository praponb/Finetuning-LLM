from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .groupBy(
        F.when(F.col("city").isin(["London", "Bangkok"]), "group_a").otherwise("other").alias("city_group"),
    )
    .agg(F.sum("age").alias("total_age"))
)
