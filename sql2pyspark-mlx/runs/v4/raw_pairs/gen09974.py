from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date", "id")

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") > 80)
    .select(
        "id",
        "status",
        "end_date",
        "monthly_fee",
        (F.col("monthly_fee") - F.lag("monthly_fee", 1).over(w)).alias("monthly_fee_change"),
    )
    .orderBy(F.col("monthly_fee").desc(), F.col("monthly_fee_change").desc())
)
