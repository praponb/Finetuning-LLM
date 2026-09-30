from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance", "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("balance").desc(), "id")

result = (
    df_accounts
    .select(
        "id",
        "status",
        "balance",
        "customer_id",
        "opened_date",
        F.row_number().over(w).alias("row_num"),
        F.first("opened_date").over(w2).alias("first_opened_date"),
    )
)
