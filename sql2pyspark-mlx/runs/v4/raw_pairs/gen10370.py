from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc(), "id")

filtered = (
    df_customers
    .filter(F.col("signup_date") >= F.lit("2023-08-05").cast("date"))
    .select("*", F.length("country").alias("country_length"))
)

kept = (
    filtered
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.opened_date").between("2023-06-01", "2025-02-01")), "left_semi")
)

latest = (
    kept
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .groupBy("country")
    .agg(
        F.bit_or("age").alias("age_bit_or"),
        F.bit_and("age").alias("age_bit_and"),
    )
    .filter(F.col("age_bit_and") > 1)
    .orderBy(F.col("age_bit_and").desc_nulls_last())
)
