from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date", "id")

filtered = (
    df_accounts
    .filter(F.col("opened_date") >= F.lit("2023-07-09").cast("date"))
    .select(
        "id",
        "account_type",
        "opened_date",
        F.when(F.col("status").isin(["OPEN", "FROZEN"]), "Priority").otherwise("Other").alias("status_group"),
    )
)

result = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)
