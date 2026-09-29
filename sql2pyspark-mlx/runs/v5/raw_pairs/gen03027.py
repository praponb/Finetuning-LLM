from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc_nulls_first())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

result = flattened.withColumn("tier_value_dense_rank", F.dense_rank().over(w))
