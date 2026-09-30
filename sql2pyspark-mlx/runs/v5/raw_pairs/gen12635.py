from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date")

result = (
    df_accounts
    .select("id", "opened_date", F.percent_rank().over(w).alias("opened_date_pct_rank"))
)
