from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("currency")
w_grp_2 = Window.orderBy("element")

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("element"),
    )
)

result = (
    flattened
    .select(
        "plan",
        "effective_from",
        "currency",
        F.max("element").over(w_grp).alias("currency_max_element"),
        F.stddev_samp("element").over(w_grp).alias("currency_stddev_samp_element"),
        F.cume_dist().over(w_grp_2).alias("element_cume_dist"),
    )
)
