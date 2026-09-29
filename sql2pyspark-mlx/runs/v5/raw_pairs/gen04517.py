from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") < 11200)
    .select(F.lit("customer").alias("origin"), "city")
    .unionByName(
        df_stores
        .filter(F.col("store_name").like("%Old%"))
        .select(F.lit("store").alias("origin"), "city")
    )
    .distinct()
    .orderBy(F.col("city").desc())
)
