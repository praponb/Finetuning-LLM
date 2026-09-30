from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment")

result = (
    df_customers
    .filter(F.col("city") == "Bangkok")
    .select(
        "id",
        "segment",
        "credit_limit",
        F.avg("credit_limit").over(w).alias("segment_avg_credit_limit"),
    )
)
