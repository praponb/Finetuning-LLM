from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date", "id")
w2 = Window.partitionBy("customer_id", "account_type").orderBy("balance", "id")

valid = (
    df_accounts
    .filter(F.col("opened_date") > F.to_date(F.lit("2023-12-23")))
    .withColumn("opened_date_plus_months", F.add_months("opened_date", 6))
    .withColumn("status_filled", F.coalesce("status", F.lit("OTHER")))
)

result = (
    valid
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("event_seq", F.row_number().over(w2))
)
