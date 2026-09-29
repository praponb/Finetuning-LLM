from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("value"),
    )
)

valid = (
    lines
    .filter(F.col("created_at") > F.lit("2025-03-02 00:00:00").cast("timestamp"))
    .select("*", F.col("created_at").cast("date").alias("created_at_day"))
)

base = (
    valid
    .filter(F.hour("created_at").between(8, 16))
    .select(
        "id",
        "customer_id",
        "created_at",
        "value",
        F.when(F.col("value").isin(["VIP", "BUNDLE"]), "Group A").otherwise("Other").alias("value_group"),
        F.add_months("created_at_day", 12).alias("created_at_day_plus_months"),
    )
)

enriched = (
    base
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.created_at",
        "a.created_at_day_plus_months",
        F.col("b.city"),
        F.col("b.name"),
        F.col("b.country"),
    )
)

result = (
    enriched
    .select(
        "id",
        "customer_id",
        "created_at_day_plus_months",
        "city",
        "name",
        "country",
        F.when(F.col("name").rlike("^(Jack Taylor|Pim Chaiyaporn)$"), "EXTENDED").when(F.col("name").rlike("^(Alice Wong|bob smith)$"), "PRIORITY").otherwise("CORE").alias("name_zone"),
    )
)
