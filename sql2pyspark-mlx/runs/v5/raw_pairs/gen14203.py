from pyspark.sql import functions as F

base = (
    df_customers
    .filter(F.col("segment").isNotNull() & F.col("credit_limit").between(12600, 15800))
    .withColumn("age_filled", F.coalesce("age", F.lit(0)))
)

result = (
    base
    .select("id")
    .intersect(
        df_customers
        .filter(F.col("name").isNotNull())
        .select("id")
    )
)
