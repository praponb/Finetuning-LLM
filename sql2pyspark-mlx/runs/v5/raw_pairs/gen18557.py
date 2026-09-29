from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date")

result = (
    df_accounts
    .filter(F.col("customer_id").isin([6, 13, 34, 40]))
    .select("id", "customer_id", "opened_date", F.rank().over(w).alias("rnk"))
    .orderBy("opened_date")
)
