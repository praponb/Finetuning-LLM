from pyspark.sql import functions as F

result = (
    df_customers
    .withColumn("is_active_label", F.when(F.col("is_active"), "Yes").otherwise("No"))
    .orderBy(F.col("signup_date").desc())
)
