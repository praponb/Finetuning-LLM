from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_last())
w2 = Window.partitionBy("plan")

flattened = df_price_tiers.select("id", "plan", F.explode("tier_prices").alias("val"))

result = (
    flattened
    .select(
        "id",
        "plan",
        F.rank().over(w).alias("val_rank"),
        F.sum("val").over(w2).alias("plan_sum_val"),
    )
)
