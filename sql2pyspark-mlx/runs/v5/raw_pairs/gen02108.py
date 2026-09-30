from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("opened_date_period").desc())

valid = (
    df_accounts
    .filter(F.col("status") == "FROZEN")
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "status",
        F.date_trunc("year", "opened_date").alias("opened_date_period"),
        F.when(F.col("balance") >= 86000, "High").when(F.col("balance") >= 13000, "Medium").otherwise("Low").alias("balance_tier"),
    )
)

result = (
    valid
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "customer_id", "opened_date", "status")
)
