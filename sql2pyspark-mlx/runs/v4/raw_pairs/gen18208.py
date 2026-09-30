from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("opened_date").desc(), "id")

result = (
    df_accounts
    .filter(F.col("opened_date") <= "2024-10-01")
    .select(
        "id",
        "status",
        "opened_date",
        "customer_id",
        F.first("customer_id").over(w).alias("first_customer_id"),
    )
)
