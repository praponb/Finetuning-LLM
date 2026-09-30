from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy(F.col("effective_from_day_minus_val").desc())

flattened = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("tier_prices").alias("val"))
)

cleaned = (
    flattened
    .filter(F.col("plan").isin(["basic", "enterprise"]))
    .select("*", F.dayofmonth("effective_from").alias("effective_from_day"))
)

filtered = (
    cleaned
    .filter(F.col("plan").isin(["basic", "starter"]))
    .select(
        "*",
        (F.col("effective_from_day") - F.col("val")).alias("effective_from_day_minus_val"),
    )
)

metrics = (
    filtered
    .withColumn("plan_min_effective_from_day_minus_val", F.min("effective_from_day_minus_val").over(w))
    .withColumn("plan_count_val", F.count("val").over(w))
    .withColumn("effective_from_day_minus_val_rank", F.rank().over(w2))
)

result = (
    metrics
    .select(
        "id",
        "plan",
        "effective_from",
        "val",
        "effective_from_day",
        "effective_from_day_minus_val",
        "plan_min_effective_from_day_minus_val",
        "plan_count_val",
        "effective_from_day_minus_val_rank",
        F.when(F.col("plan").rlike("^(basic|enterprise)$"), "GROUP_A").when(F.col("plan").rlike("^(starter|pro)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("plan_zone"),
    )
)
