from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("status").orderBy(F.col("balance").desc())
w2 = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(-5, -1)

result = (
    df_accounts
    .filter(F.col("account_type") == "CHECKING")
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        F.cume_dist().over(w_ordered).alias("balance_cume_dist"),
        F.avg("balance").over(w2).alias("balance_avg_5_prior"),
    )
)
