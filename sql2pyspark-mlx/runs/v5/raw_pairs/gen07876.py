from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("end_date").desc(), "id")
w2 = Window.partitionBy("plan")

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "end_date",
        "monthly_fee",
        F.row_number().over(w).alias("rn"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 3).alias("monthly_fee_share"),
    )
    .orderBy(F.col("id").desc(), F.col("rn").desc())
)
