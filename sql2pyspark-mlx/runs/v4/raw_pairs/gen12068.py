from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") == 39)
    .select(
        "age",
        "segment",
        F.add_months("signup_date", 1).alias("signup_date_plus_months"),
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
    .orderBy(F.col("segment").desc())
)
