from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_payments
    .filter(F.col("method").isin(["cash", "bank_transfer"]))
    .select(
        "*",
        F.lower("method").alias("method_lower"),
        F.datediff(F.current_date(), "paid_at").alias("days_since_paid_at"),
        F.coalesce("method", F.lit("N/A")).alias("resolved_method"),
    )
)

result = (
    valid
    .filter(F.col("amount") < 850)
    .select(
        "id",
        "amount",
        "paid_at",
        "status",
        "method_lower",
        "days_since_paid_at",
        "resolved_method",
        F.max("days_since_paid_at").over(w).alias("running_max_days_since_paid_at"),
        F.count("days_since_paid_at").over(w).alias("running_count_days_since_paid_at"),
        F.min("days_since_paid_at").over(w).alias("running_min_days_since_paid_at"),
        F.avg("days_since_paid_at").over(w).alias("running_avg_days_since_paid_at"),
    )
)
