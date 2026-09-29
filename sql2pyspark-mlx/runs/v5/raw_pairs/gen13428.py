from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("balance", "id")

result = (
    df_accounts
    .filter(F.col("customer_id").isin([22, 39]))
    .select("id", "customer_id", "balance", F.row_number().over(w).alias("rn"))
)
