from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("opened_date").desc(), "id")

result = (
    df_accounts
    .filter(F.col("account_type").isin(["LOAN", "CREDIT"]))
    .select("id", "customer_id", "opened_date", F.row_number().over(w).alias("row_num"))
)
