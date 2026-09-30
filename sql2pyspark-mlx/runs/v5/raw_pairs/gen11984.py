from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").desc_nulls_first())

flattened = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("tier_prices").alias("element"))
)

valid = (
    flattened
    .filter(F.col("effective_from").between("2024-04-01", "2024-06-01"))
    .withColumn("adjusted_element", F.round((F.col("element") * 1.1), 2))
)

with_windows = (
    valid
    .filter(F.col("adjusted_element") >= 44)
    .withColumn("element_dense_rank", F.dense_rank().over(w))
)

base = (
    with_windows
    .filter(F.col("adjusted_element") >= 67)
    .select(
        "id",
        "effective_from",
        "element",
        "adjusted_element",
        "element_dense_rank",
        F.date_add("effective_from", 31).alias("effective_from_plus_31d"),
    )
)

result = (
    base
    .select(
        "id",
        "effective_from",
        "element",
        "adjusted_element",
        "element_dense_rank",
        "effective_from_plus_31d",
    )
    .orderBy("effective_from", "id")
)
