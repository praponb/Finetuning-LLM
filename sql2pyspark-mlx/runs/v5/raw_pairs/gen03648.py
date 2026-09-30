from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("opened_date").desc(), "id")

result = (
    df_accounts
    .filter(~F.col("account_type").isin(["LOAN", "CHECKING"]))
    .select("id", "status", "opened_date", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .orderBy("rnk")
)
