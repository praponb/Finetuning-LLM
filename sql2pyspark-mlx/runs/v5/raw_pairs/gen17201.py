from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("age").asc_nulls_last(), "id")

latest = (
    df_customers
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .withColumn("age_tier", F.when(F.col("age") >= 71, "PREMIUM").otherwise("STANDARD"))
    .withColumn("credit_limit_tier", F.when(F.col("credit_limit") >= 12500, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["age_tier", "credit_limit_tier"], ["age_tier"]],
        "age_tier",
        "credit_limit_tier",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.countDistinct("country").alias("distinct_countries"),
        F.bit_and("age").alias("age_bit_and"),
        F.count_if(F.col("city") == "New York").alias("new_york_events"),
    )
    .orderBy(F.col("age_bit_and").desc())
)
