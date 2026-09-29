from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("effective_from", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("entry_price", "id").rowsBetween(-3, -1)

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_prices",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    flattened_attrs
    .withColumn("running_entry_price", F.sum("entry_price").over(w))
    .withColumn("entry_price_avg_3_prior", F.avg("entry_price").over(w2))
)
