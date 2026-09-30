from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "id",
        "email",
        "is_active",
        F.when(F.col("country").isin(["JP", "DE"]), "Group A").otherwise("Other").alias("country_group"),
    )
)
