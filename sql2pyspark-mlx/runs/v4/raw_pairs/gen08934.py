from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "account_type").orderBy("opened_date", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("status").orderBy("opened_date", "id")

scored = (
    df_accounts
    .select(
        "*",
        F.avg("balance").over(w).alias("balance_avg_6_prior"),
        F.lag("balance", 2).over(w2).alias("prev_balance"),
    )
)

result = (
    scored
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "balance_avg_6_prior",
        "prev_balance",
        F.when(F.col("account_type").rlike("^(CREDIT|CHECKING)$"), "GROUP_B").when(F.col("account_type").rlike("^(SAVINGS|LOAN)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("account_type_zone"),
    )
    .orderBy(F.col("customer_id").asc_nulls_first())
)
