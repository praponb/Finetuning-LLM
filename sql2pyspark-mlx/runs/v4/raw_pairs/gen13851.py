from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .agg(
        (F.max("age") - F.min("age")).alias("age_range"),
        F.sum("age").alias("total_age"),
    )
)
