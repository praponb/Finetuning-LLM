from pyspark.sql import functions as F

flattened = df_price_tiers.select("id", "plan", F.explode("discounts").alias("element"))

result = (
    flattened
    .groupBy(
        F.when(F.col("element") >= 88, "LARGE").otherwise("SMALL").alias("element_tier"),
        "plan",
    )
    .agg(
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles"),
        F.round(F.avg("element"), 2).alias("avg_element"),
        F.min("element").alias("min_element"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("num_rows") > 1)
    .orderBy("min_element")
)
