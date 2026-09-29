from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("opened_date").desc())

result = (
    df_accounts
    .filter(F.col("opened_date") >= "2024-02-26")
    .select(
        "id",
        "account_type",
        "opened_date",
        F.percent_rank().over(w).alias("opened_date_pct_rank"),
    )
)
