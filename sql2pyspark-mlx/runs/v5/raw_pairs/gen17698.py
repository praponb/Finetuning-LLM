from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") < 78000) & (F.col("opened_date") > "2024-12-01"))
    .groupBy("status", F.year("opened_date").alias("opened_year"))
    .agg(F.min("opened_date").alias("first_opened_date"))
    .orderBy(F.col("first_opened_date").desc(), F.col("status").desc())
)
