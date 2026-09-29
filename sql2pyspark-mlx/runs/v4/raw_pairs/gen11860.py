from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("plan")

lines = df_price_tiers.select("id", "plan", F.explode("discounts").alias("val"))

with_windows = (
    lines
    .withColumn("plan_max_val", F.max("val").over(w_part))
    .withColumn("plan_avg_val", F.avg("val").over(w_part))
)

remaining_keys = (
    with_windows
    .select("plan")
    .subtract(
        df_price_tiers
        .filter(F.col("currency") == "EUR")
        .select("plan")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

remaining_keys_2 = (
    kept_rows
    .select("id", "plan")
    .intersect(
        df_price_tiers
        .filter(F.col("currency").isin(["THB", "EUR"]))
        .select("id", "plan")
    )
)

verified = (
    kept_rows
    .alias("s")
    .join(remaining_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("plan", "val", "plan_max_val", "plan_avg_val")
    .orderBy(F.col("plan_avg_val").desc())
)
