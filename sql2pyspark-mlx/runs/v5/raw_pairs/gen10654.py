from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("status").orderBy("amount", "id")
w2 = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method")
w4 = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "amount",
        F.row_number().over(w_grp).alias("event_seq"),
        F.first("method", ignorenulls=True).over(w2).alias("first_known_method"),
        F.row_number().over(w_grp).alias("row_seq"),
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share"),
        F.ntile(3).over(w4).alias("amount_bucket"),
    )
)
