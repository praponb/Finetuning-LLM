from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("country").isin(["SG", "JP"]), "core").otherwise("other").alias("country_group"),
        "city",
    )
    .agg(F.sum("age").alias("total_age"))
    .filter(F.col("total_age") < 360)
    .select("country_group", "city", "total_age")
    .orderBy(F.col("country_group").desc(), F.col("total_age").desc())
)
