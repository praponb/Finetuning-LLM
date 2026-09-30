from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy("opened_date", "id")

result = (
    df_accounts
    .filter(F.col("customer_id").isin([2, 5]))
    .select(
        "id",
        "customer_id",
        "status",
        "opened_date",
        F.lead("opened_date", 2).over(w).alias("next_opened_date"),
    )
)
