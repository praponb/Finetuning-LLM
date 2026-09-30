from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance", "id")

result = (
    df_accounts
    .filter(~F.col("account_type").isin(["LOAN", "CREDIT"]))
    .select("id", "status", "balance", F.row_number().over(w).alias("rn"))
)
