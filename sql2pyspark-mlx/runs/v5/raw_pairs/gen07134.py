from pyspark.sql import functions as F

filtered = (
    df_promotions
    .filter((F.col("ends_on") > "2025-09-01") & F.col("channel").isNotNull())
    .select(
        "id",
        "promo_code",
        "starts_on",
        "discount_pct",
        F.col("discount_pct").cast("int").alias("discount_pct_int"),
    )
)

result = (
    filtered
    .select("id")
    .subtract(
        df_promotions
        .filter(F.col("discount_pct") >= 0.24)
        .select("id")
    )
)
