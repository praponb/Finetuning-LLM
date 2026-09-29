from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("end_date")

result = (
    df_subscriptions
    .filter(F.col("end_date").between("2023-10-01", "2025-01-01"))
    .select("id", "customer_id", "end_date", F.rank().over(w).alias("end_date_rank"))
)
