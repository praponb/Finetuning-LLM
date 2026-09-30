from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("opened_date").desc(), "id")

ranked = (
    df_accounts
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "account_type", "opened_date", "status")
)

result = (
    ranked
    .select(
        "id",
        "opened_date",
        "status",
        F.when(F.col("status").rlike("^(CLOSED|OPEN)$"), "EXTENDED").when(F.col("status").rlike("^(FROZEN)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("status_zone"),
    )
)
