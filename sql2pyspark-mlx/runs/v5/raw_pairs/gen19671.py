from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc(), "id")

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(120, 140))
    .select(
        "id",
        "plan",
        "monthly_fee",
        "start_date",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 5)
    .select("id", "plan", "monthly_fee", "start_date")
    .orderBy("start_date")
)
