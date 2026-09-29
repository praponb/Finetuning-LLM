from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "customer_id").orderBy("opened_date", "id")

result = (
    df_accounts
    .filter(F.col("customer_id") == 11)
    .select(
        "id",
        "account_type",
        "customer_id",
        "opened_date",
        "status",
        F.first("status").over(w).alias("first_status"),
    )
    .orderBy("first_status", F.col("customer_id").desc())
)
