from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance", "id")

result = (
    df_accounts
    .select(
        "id",
        "status",
        "balance",
        "customer_id",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") == 1)
    .select("id", "status", "balance", "customer_id")
    .orderBy(F.col("balance").desc(), F.col("status").desc())
)
