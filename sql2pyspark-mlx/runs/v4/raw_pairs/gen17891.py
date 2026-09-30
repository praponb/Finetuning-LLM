from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@startup.io"))
    .groupBy("is_active")
    .agg(F.countDistinct("age").alias("unique_countrys"))
    .filter(F.col("unique_countrys") >= 7)
)
