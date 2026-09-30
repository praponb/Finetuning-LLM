from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").desc())
w2 = Window.partitionBy("currency")

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("element"),
    )
)

with_windows = (
    lines
    .select(
        "effective_from",
        "currency",
        "element",
        F.dense_rank().over(w).alias("element_dense_rank"),
        F.avg("element").over(w2).alias("currency_avg_element"),
    )
)

valid = (
    with_windows
    .filter(F.col("element").between(46, 72))
    .select(
        "effective_from",
        "currency",
        "element",
        "element_dense_rank",
        "currency_avg_element",
        (F.col("element") / F.nullif("currency_avg_element", F.lit(0))).alias("element_per_currency_avg_element"),
        F.when(F.col("element_dense_rank") >= 7, "High").when(F.col("element_dense_rank") >= 3, "Medium").otherwise("Low").alias("element_dense_rank_band"),
    )
)

metrics = (
    valid
    .select(
        "currency",
        "currency_avg_element",
        "element_per_currency_avg_element",
        "element_dense_rank_band",
        F.percent_rank().over(w).alias("element_percent_rank"),
        F.round((F.col("element_dense_rank") / F.sum("element_dense_rank").over(w2)), 4).alias("element_dense_rank_share"),
    )
)

result = (
    metrics
    .select(
        "currency",
        "element_dense_rank_share",
        "element_per_currency_avg_element",
        "element_percent_rank",
    )
    .unpivot(
        ids=["currency"],
        values=["element_dense_rank_share", "element_per_currency_avg_element", "element_percent_rank"],
        variableColumnName="kpi",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
