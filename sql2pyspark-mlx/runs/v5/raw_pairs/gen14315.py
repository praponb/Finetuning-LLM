from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("status", "customer_id").orderBy("balance", "id")
w2 = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_accounts
    .filter(F.col("opened_date") >= F.lit("2023-04-20").cast("date"))
    .withColumn("is_checking", F.when(F.col("account_type") == "CHECKING", 1).otherwise(0))
    .withColumn("account_type_prefix", F.substring("account_type", 1, 4))
)

result = (
    valid
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "is_checking",
        "account_type_prefix",
        F.lead("is_checking").over(w_ordered).alias("next_is_checking"),
        F.lead("balance").over(w_ordered).alias("next_balance"),
        F.lag("balance").over(w_ordered).alias("prev_balance"),
        F.row_number().over(w_ordered).alias("row_seq"),
        F.sum("balance").over(w2).alias("cumulative_balance"),
    )
)
