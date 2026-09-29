from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("balance", "id")

result = (
    df_accounts
    .select("id", "customer_id", "balance", F.row_number().over(w).alias("row_num"))
)
