from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy(F.col("monthly_fee").desc_nulls_first())
w_grp = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc_nulls_last())
w3 = Window.partitionBy("status")
w4 = Window.orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id")

scored = (
    df_subscriptions
    .select(
        "*",
        F.cume_dist().over(w_part).alias("monthly_fee_cume_dist"),
        F.dense_rank().over(w_grp).alias("monthly_fee_dense_rank"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w3)), 4).alias("monthly_fee_share"),
        F.sum("monthly_fee").over(w4).alias("running_monthly_fee"),
        F.row_number().over(w5).alias("seq_no"),
    )
)

result = (
    scored
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee_cume_dist",
        "monthly_fee_dense_rank",
        "running_monthly_fee",
        F.when(F.col("plan").rlike("^(basic|premium)$"), "EXTENDED").when(F.col("plan").rlike("^(standard|enterprise)$"), "GROUP_B").otherwise("CORE").alias("plan_zone"),
    )
    .orderBy(F.col("monthly_fee_cume_dist").desc())
)
