from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("opened_date").desc())

result = (
    df_accounts
    .select("id", "account_type", "opened_date", F.rank().over(w).alias("rnk"))
)
