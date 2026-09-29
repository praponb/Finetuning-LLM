from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("opened_date", "id")

metrics = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "opened_date",
        "status",
        F.sum("balance").over(w).alias("cumulative_balance"),
        F.lead("balance", 1, 0).over(w2).alias("next_balance"),
    )
)

result = (
    metrics
    .select(
        "id",
        "customer_id",
        "opened_date",
        "status",
        "cumulative_balance",
        F.when(F.col("status").rlike("^(OPEN|FROZEN)$"), "GROUP_A").otherwise("GROUP_B").alias("status_zone"),
    )
    .orderBy(F.col("status").desc_nulls_first())
)
