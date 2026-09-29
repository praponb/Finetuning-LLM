from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy(F.col("opened_date").desc(), "id")

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "status",
        "opened_date",
        F.row_number().over(w).alias("row_num"),
    )
    .orderBy("opened_date")
)
