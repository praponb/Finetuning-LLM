from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("location").isin(["London", "Berlin"]), "tier_1").otherwise("other").alias("location_group"),
    )
    .agg(F.sum("budget").alias("total_budget"))
)
