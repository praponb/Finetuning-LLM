from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("balance").desc(), "id")

result = (
    df_accounts
    .filter(~F.col("account_type").isin(["CREDIT", "CHECKING"]))
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 3)
    .select("id", "customer_id", "balance", "opened_date")
    .orderBy("opened_date")
)
