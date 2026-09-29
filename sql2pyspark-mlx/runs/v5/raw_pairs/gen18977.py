from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("monthly_fee", "id")

result = (
    df_subscriptions
    .select(
        "id",
        "status",
        "monthly_fee",
        "end_date",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 3)
)
