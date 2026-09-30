from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("account_type").orderBy(F.col("balance").desc_nulls_last())
w2 = Window.partitionBy("status", "account_type").orderBy("opened_date", "id")
w_recent = Window.partitionBy("customer_id").orderBy(F.col("opened_date").desc(), "id")

enriched = (
    df_accounts
    .withColumn("balance_percent_rank", F.percent_rank().over(w_part))
    .withColumn("next_balance", F.lead("balance", 1).over(w2))
    .withColumn("seq_no", F.row_number().over(w_recent))
)

result = (
    enriched
    .select(
        "id",
        "account_type",
        "opened_date",
        "status",
        "next_balance",
        "seq_no",
        F.when(F.col("status").rlike("^(FROZEN|CLOSED)$"), "OTHER_ZONE").otherwise("CORE").alias("status_zone"),
    )
    .orderBy(F.col("opened_date").desc_nulls_last())
)
