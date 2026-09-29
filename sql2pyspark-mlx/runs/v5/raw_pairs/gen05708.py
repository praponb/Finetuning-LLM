from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id")
w2 = Window.partitionBy("status").orderBy("opened_date", "id")

result = (
    df_accounts
    .filter(F.col("account_type").isin(["CHECKING", "SAVINGS", "LOAN"]))
    .select(
        "id",
        "opened_date",
        "status",
        F.lead("opened_date", 2).over(w).alias("next_opened_date"),
        F.row_number().over(w2).alias("opened_date_row"),
    )
)
