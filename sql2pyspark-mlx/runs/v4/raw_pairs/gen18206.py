from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "id",
        "city",
        "credit_limit",
        "country",
        F.initcap(F.trim("segment")).alias("segment_title"),
    )
    .orderBy(F.col("id").desc())
    .limit(5)
)
