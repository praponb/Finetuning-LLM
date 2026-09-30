from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("balance").desc())

result = (
    df_accounts
    .filter(F.year("opened_date") == 2025)
    .select(
        "id",
        "status",
        "balance",
        F.percent_rank().over(w).alias("balance_pct_rank"),
    )
)
