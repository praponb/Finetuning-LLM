from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date")

result = (
    df_accounts
    .filter(F.col("account_type").isin(["CREDIT", "CHECKING"]))
    .select(
        "id",
        "account_type",
        "opened_date",
        F.percent_rank().over(w).alias("opened_date_pct_rank"),
    )
    .orderBy("account_type")
)
