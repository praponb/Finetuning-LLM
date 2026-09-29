from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy(F.col("balance").desc_nulls_first())
w3 = Window.orderBy(F.col("balance").desc_nulls_first())
w4 = Window.partitionBy("status", "customer_id").orderBy("opened_date", "id").rowsBetween(-6, -1)
w5 = Window.orderBy(F.col("balance").asc_nulls_first())

result = (
    df_accounts
    .select(
        "*",
        F.first("balance", ignorenulls=True).over(w).alias("first_known_balance"),
        F.rank().over(w2).alias("balance_rank"),
        F.rank().over(w3).alias("balance_rank_2"),
        F.avg("balance").over(w4).alias("balance_avg_6_prior"),
        F.rank().over(w5).alias("balance_rank_3"),
    )
)
