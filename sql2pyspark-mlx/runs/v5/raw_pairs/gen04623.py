from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("country").isin(["TH", "SG"]), "priority").otherwise("other").alias("country_group"),
    )
    .agg(F.sum("credit_limit").alias("total_credit_limit"))
)
