from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("balance").desc(), "id")

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        F.first("opened_date").over(w).alias("first_opened_date"),
    )
)
