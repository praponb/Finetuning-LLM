from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "account_type").orderBy("opened_date_year", "id")

valid = (
    df_accounts
    .filter(F.col("opened_date") >= F.lit("2023-03-31").cast("date"))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "status",
        F.year("opened_date").alias("opened_date_year"),
        F.coalesce("status", F.lit("DIRECT")).alias("status_filled"),
    )
)

result = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)
