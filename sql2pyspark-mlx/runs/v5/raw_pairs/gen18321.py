from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("start_date").asc_nulls_first())

ranked = (
    df_subscriptions
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "rn",
        F.when(F.col("plan").rlike("^(premium|basic)$"), "GROUP_A").otherwise("GROUP_B").alias("plan_zone"),
    )
    .orderBy(F.col("monthly_fee").desc_nulls_first())
)
