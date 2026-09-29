from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("email").like("%@startup.io"))
    .select(
        "id",
        "age",
        F.concat("segment", F.lit(" "), "country").alias("segment_country"),
    )
    .orderBy(F.col("age").desc())
)
