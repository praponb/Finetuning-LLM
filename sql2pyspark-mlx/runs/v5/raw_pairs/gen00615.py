from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "is_active",
        "signup_date",
        F.when(F.col("segment").isin(["Consumer", "Corporate"]), "Priority").otherwise("Other").alias("segment_group"),
    )
    .orderBy("segment_group")
)
