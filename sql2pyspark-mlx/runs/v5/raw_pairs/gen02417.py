from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("country") != "DE") & F.col("email").like("%@mail.co.th"))
    .select(
        "name",
        F.when(F.col("country").isin(["SG", "US"]), "Priority").otherwise("Other").alias("country_group"),
    )
    .orderBy(F.col("name").desc())
)
