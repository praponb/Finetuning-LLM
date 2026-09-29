from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("status").isin(["FROZEN", "OPEN"]) & (F.col("account_type") == "CREDIT"))
    .withColumn("opened_date_label", F.date_format("opened_date", "yyyy-MM"))
    .withColumn("is_open", F.when(F.col("status") == "OPEN", 1).otherwise(0))
    .orderBy(F.col("status").desc())
)
