from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNull() & F.col("opened_date").between("2023-08-04", "2023-12-01") & F.month("opened_date").isin([3, 5, 12]))
    .withColumn("opened_date_month_end", F.last_day("opened_date"))
    .orderBy(F.col("id").desc())
)
