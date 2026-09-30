from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc(), "id")

result = (
    df_subscriptions
    .filter(F.col("plan") == "standard")
    .select("id", "customer_id", "monthly_fee", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 5)
    .select("id", "customer_id", "monthly_fee")
)
