from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("balance").desc())

result = (
    df_accounts
    .filter(F.col("opened_date").between("2023-11-03", "2024-03-01"))
    .select(
        "id",
        "customer_id",
        "balance",
        F.dense_rank().over(w).alias("balance_dense_rank"),
    )
    .orderBy(F.col("id").desc())
)
