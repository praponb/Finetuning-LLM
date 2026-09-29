from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("amount_int")
w3 = Window.partitionBy("status", "method").orderBy("amount_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("method", "status").orderBy(F.col("paid_at").desc(), "id")

valid = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-04-28 11:00:00").cast("timestamp")) & F.col("method").isNotNull())
    .select(
        "id",
        "method",
        "paid_at",
        "status",
        F.to_date("paid_at").alias("paid_at_date"),
        F.col("amount").cast("int").alias("amount_int"),
    )
)

result = (
    valid
    .select(
        "id",
        "method",
        "status",
        "paid_at_date",
        "amount_int",
        F.first("method", ignorenulls=True).over(w).alias("first_known_method"),
        F.cume_dist().over(w2).alias("amount_int_cume_dist"),
        F.first("method", ignorenulls=True).over(w3).alias("first_known_method_2"),
        F.row_number().over(w4).alias("row_seq"),
    )
)
