from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date", "id")

result = (
    df_accounts
    .filter(F.col("balance") >= 64000)
    .select(
        "id",
        "status",
        "opened_date",
        "balance",
        F.lag("balance", 1, 0).over(w).alias("prev_balance"),
    )
)
