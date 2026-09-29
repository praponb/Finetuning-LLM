from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id")

result = (
    df_accounts
    .filter(F.col("opened_date") < "2023-04-01")
    .select(
        "id",
        "customer_id",
        "opened_date",
        "balance",
        F.lead("balance", 2).over(w).alias("next_balance"),
    )
)
