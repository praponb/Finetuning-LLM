from pyspark.sql import functions as F

result = (
    df_accounts
    .withColumn("days_since_opened_date", F.datediff(F.current_date(), "opened_date"))
    .orderBy(F.col("id").desc())
)
