from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part_2 = Window.partitionBy("method").orderBy(F.col("amount").desc())

metrics = (
    df_payments
    .select(
        "*",
        F.max("amount").over(w_part).alias("running_max_amount"),
        F.count("amount").over(w_part).alias("running_count_amount"),
        F.sum("amount").over(w_part).alias("running_sum_amount"),
        F.cume_dist().over(w_part_2).alias("amount_cume_dist"),
    )
)

result = (
    metrics
    .select(
        "id",
        "amount",
        "status",
        "running_max_amount",
        "running_sum_amount",
        F.when(F.col("status").rlike("^(REFUNDED|FAILED)$"), "PRIORITY").when(F.col("status").rlike("^(SUCCESS)$"), "CORE").otherwise("GROUP_B").alias("status_zone"),
    )
)
