from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc())

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") > 170)
    .select(
        "id",
        "status",
        "monthly_fee",
        F.percent_rank().over(w).alias("monthly_fee_pct_rank"),
    )
)
