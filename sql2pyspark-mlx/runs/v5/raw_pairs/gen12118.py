from pyspark.sql import functions as F

long_format = (
    df_orders
    .select("id", "channel", "shipping_fee", "amount", "discount")
    .unpivot(
        ids=["id", "channel"],
        values=["shipping_fee", "amount", "discount"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)

result = (
    long_format
    .groupBy(
        "channel",
        F.when(F.col("reading") >= 74, "HIGH").otherwise("LOW").alias("reading_tier"),
    )
    .agg(
        F.sum(F.when(F.col("measure") == "discount", F.col("reading")).otherwise(0)).alias("discount_reading"),
        F.round(F.sum("reading") / F.nullif(F.count(F.when(F.col("channel") == "web", 1)), F.lit(0)), 2).alias("reading_per_web"),
        F.percentile_approx("reading", [0.25, 0.5, 0.75]).alias("reading_quartiles"),
        F.percentile_approx("reading", 0.9).alias("reading_p90"),
    )
    .filter(F.col("reading_p90") > 0)
    .orderBy(F.col("discount_reading").desc())
)
