from pyspark.sql import functions as F

lines = df_price_tiers.select("id", "plan", F.explode_outer("tier_limits").alias("val"))

filtered = (
    lines
    .filter(F.col("plan") != "enterprise")
    .select("*", F.col("val").cast("string").alias("val_str"))
)

cleaned = (
    filtered
    .filter(F.col("plan").isin(["enterprise", "starter"]))
    .withColumn("val_str_2", F.col("val").cast("double"))
)

valid = (
    cleaned
    .filter(F.col("val").between(21, 40))
    .select("plan", "val", "val_str", "val_str_2", F.ceil("val").alias("val_whole"))
)

result = valid.select("val_str", "val_str_2", "val_whole")
