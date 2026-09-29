from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city", F.date_trunc("QUARTER", "signup_date").alias("quarter_start"))
    .agg(
        F.count("*").alias("num_rows"),
        F.bit_and("age").alias("age_bit_and"),
    )
    .filter(F.col("num_rows") >= 1)
)
