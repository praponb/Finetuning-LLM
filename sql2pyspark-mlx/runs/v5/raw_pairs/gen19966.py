from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy(F.col("opened_date").desc())

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "status",
        "opened_date",
        F.dense_rank().over(w).alias("opened_date_dense_rank"),
    )
)
