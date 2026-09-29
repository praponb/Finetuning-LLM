from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc(), "id")

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([10, 18, 26, 37]))
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        "status",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 3)
    .select("id", "customer_id", "monthly_fee", "status")
)
