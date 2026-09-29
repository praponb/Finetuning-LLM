from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w_part = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(-3, Window.currentRow)
w_part_2 = Window.orderBy("paid_at", "id")
w_part_3 = Window.partitionBy("method")

enriched = (
    df_payments
    .filter(F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.avg("amount").over(w_part).alias("amount_avg_3"),
        F.row_number().over(w_part_2).alias("row_seq"),
        F.sum("amount").over(w_part_3).alias("method_sum_amount"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("row_seq") >= 8, "LARGE").otherwise("SMALL").alias("row_seq_tier"),
        F.date_trunc("QUARTER", "paid_at").alias("quarter_start"),
    )
    .agg(
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_rows"),
        F.percentile_approx("amount_share", 0.5).alias("amount_share_p50"),
        F.percentile_approx("amount_avg_3", 0.9).alias("amount_avg_3_p90"),
        F.round(F.sum("row_seq") / F.nullif(F.count(F.when(F.col("method") == "paypal", 1)), F.lit(0)), 2).alias("row_seq_per_paypal"),
        F.avg("row_seq").alias("avg_row_seq"),
    )
)
