from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant")
w_grp = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("txn_type").orderBy("amount", "id")
w4 = Window.partitionBy("merchant").orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .select(
        "*",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.sum("amount").over(w_grp).alias("running_amount"),
        F.ntile(3).over(w_ordered).alias("amount_bucket"),
        F.sum("amount").over(w_grp).alias("running_amount_2"),
        F.ntile(3).over(w4).alias("amount_bucket_2"),
    )
)
