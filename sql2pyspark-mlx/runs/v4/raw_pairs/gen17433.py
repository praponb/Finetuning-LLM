from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("element")
w2 = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_last())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("discounts").alias("element"),
    )
)

with_windows = (
    exploded
    .withColumn("element_cume_dist", F.cume_dist().over(w))
    .withColumn("element_cume_dist_2", F.cume_dist().over(w2))
)

base = (
    with_windows
    .filter(F.col("element_cume_dist_2") >= 0.52)
    .select(
        "id",
        "plan",
        "effective_from",
        "element_cume_dist",
        "element_cume_dist_2",
        F.initcap(F.trim("plan")).alias("plan_title"),
    )
)

result = (
    base
    .select(
        "plan",
        F.col("element_cume_dist_2").alias("ELEMENT_CUME_DIST_2_VAL"),
        F.col("element_cume_dist").alias("ELEMENT_CUME_DIST"),
    )
    .unpivot(
        ["plan"],
        ["ELEMENT_CUME_DIST_2_VAL", "ELEMENT_CUME_DIST"],
        "metric",
        "val",
    )
    .filter(F.col("val").isNotNull())
)
