from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date", "id")

result = (
    df_accounts
    .select("id", "account_type", "opened_date", F.ntile(3).over(w).alias("quartile"))
    .orderBy("account_type", F.col("id").desc())
)
