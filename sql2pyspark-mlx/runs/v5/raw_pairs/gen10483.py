from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "plan").orderBy("end_date", "id")

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2023-05-01", "2024-06-28"))
    .select("id", "customer_id", "plan", "end_date", F.ntile(2).over(w).alias("bucket"))
    .orderBy("plan")
)
