from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("end_date").desc(), "id")
w2 = Window.partitionBy("plan").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc(), "id")

latest = (
    df_subscriptions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

enriched = (
    latest
    .select(
        "*",
        F.sum("monthly_fee").over(w2).alias("cumulative_monthly_fee"),
        F.ntile(5).over(w3).alias("monthly_fee_bucket"),
    )
)

remaining_keys = (
    enriched
    .select("id", "plan")
    .subtract(
        df_subscriptions
        .filter(F.col("plan") != "basic")
        .select("id", "plan")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

joined = (
    kept_rows
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.age"), F.col("b.name"), F.col("b.email"))
)

result = (
    joined
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "cumulative_monthly_fee",
        "monthly_fee_bucket",
        "age",
    )
    .orderBy(F.col("id").desc())
)
