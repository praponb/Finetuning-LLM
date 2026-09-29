from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("balance").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_accounts
    .select(
        "id",
        "account_type",
        "balance",
        "status",
        F.last("status").over(w).alias("last_status"),
    )
    .orderBy(F.col("last_status").desc(), "status")
)
