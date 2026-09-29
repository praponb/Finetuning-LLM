from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date", "id")

result = (
    df_accounts
    .select(
        "id",
        "status",
        "opened_date",
        F.lead("opened_date", 1).over(w).alias("next_opened_date"),
    )
)
