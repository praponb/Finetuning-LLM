from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "opened_date",
        "account_type",
        F.last("account_type").over(w).alias("last_account_type"),
    )
)
