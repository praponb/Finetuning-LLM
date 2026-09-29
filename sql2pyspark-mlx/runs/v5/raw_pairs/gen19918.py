from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("balance")
w2 = Window.partitionBy("customer_id").orderBy(F.col("opened_date").desc())

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        F.percent_rank().over(w).alias("balance_pct_rank"),
        F.rank().over(w2).alias("opened_date_rank"),
    )
)
