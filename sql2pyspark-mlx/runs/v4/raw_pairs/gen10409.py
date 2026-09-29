from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "plan").orderBy(F.col("is_active").desc_nulls_first())
w2 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id")
w3 = Window.partitionBy("customer_id", "plan").orderBy("end_date", "id")
w_grp = Window.partitionBy("plan").orderBy("is_active", "id")
w_ordered = Window.partitionBy("customer_id", "plan").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_subscriptions
    .filter(F.col("end_date") > F.lit("2023-12-20").cast("date"))
    .withColumn("is_active", F.when(F.col("status") == "ACTIVE", 1).otherwise(0))
)

result = (
    cleaned
    .withColumn("is_active_percent_rank", F.percent_rank().over(w))
    .withColumn("next_monthly_fee", F.lead("monthly_fee", 1).over(w2))
    .withColumn("next_is_active", F.lead("is_active", 1).over(w3))
    .withColumn("next_monthly_fee_2", F.lead("monthly_fee", 1).over(w_grp))
    .withColumn("first_known_monthly_fee", F.first("monthly_fee", ignorenulls=True).over(w_ordered))
)
