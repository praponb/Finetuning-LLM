from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("status").orderBy(F.col("opened_date").desc(), "id")

base = (
    df_accounts
    .filter(F.col("status").isin(["OPEN", "FROZEN"]))
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "status",
        F.lower("account_type").alias("account_type_lower"),
    )
)

result = (
    base
    .select(
        "id",
        "customer_id",
        "account_type",
        "status",
        F.first("status", ignorenulls=True).over(w_ordered).alias("first_known_status"),
        F.first("status", ignorenulls=True).over(w2).alias("first_known_status_2"),
        F.row_number().over(w_part).alias("row_seq"),
    )
)
