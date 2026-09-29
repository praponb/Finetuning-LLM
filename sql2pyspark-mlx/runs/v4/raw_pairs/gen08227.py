from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("end_date", "id")

result = (
    df_subscriptions
    .filter(F.col("plan") == "enterprise")
    .select(
        "id",
        "plan",
        "end_date",
        "status",
        F.first("status").over(w).alias("first_status"),
    )
)
