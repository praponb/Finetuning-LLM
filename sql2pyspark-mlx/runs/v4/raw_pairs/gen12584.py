from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("opened_date").desc(), "id")

result = (
    df_accounts
    .select(
        "id",
        "account_type",
        "opened_date",
        "status",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 3)
    .orderBy("account_type", "opened_date")
)
