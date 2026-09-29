from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "status").orderBy(F.col("opened_date").desc(), "id")

base = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "status",
        F.concat("account_type", F.lit(" "), "status").alias("account_type_status"),
        F.coalesce("status", F.lit("OTHER")).alias("status_clean"),
    )
)

result = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)
