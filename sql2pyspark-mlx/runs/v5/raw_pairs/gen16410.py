from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id")

result = (
    df_subscriptions
    .filter(F.col("plan") == "enterprise")
    .select(
        "id",
        "status",
        "monthly_fee",
        "end_date",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") == 1)
    .select("id", "status", "monthly_fee", "end_date")
    .orderBy(F.col("monthly_fee").desc(), "end_date")
)
