from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())

with_last = (
    df_customers
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.account_type"), F.col("c.balance").alias("account_balance"))
)

result = (
    with_last
    .select(
        "id",
        "name",
        "email",
        "segment",
        "is_active",
        "credit_limit",
        "age",
        "account_type",
        "account_balance",
    )
    .orderBy(F.col("account_balance").desc_nulls_first())
)
