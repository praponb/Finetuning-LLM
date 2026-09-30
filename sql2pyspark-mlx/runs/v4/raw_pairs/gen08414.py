from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("plan")
w2 = Window.orderBy("element")
w3 = Window.partitionBy("currency")
w4 = Window.partitionBy("plan", "currency").orderBy(F.col("element").desc_nulls_last())
w_recent = Window.partitionBy("currency", "plan")

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("discounts").alias("element"))
)

result = (
    flattened
    .select(
        "*",
        F.avg("element").over(w_part).alias("plan_avg_element"),
        F.rank().over(w2).alias("element_rank"),
        F.max("element").over(w3).alias("currency_max_element"),
        F.percent_rank().over(w4).alias("element_percent_rank"),
        F.max("element").over(w_recent).alias("currency_max_element_2"),
    )
)
