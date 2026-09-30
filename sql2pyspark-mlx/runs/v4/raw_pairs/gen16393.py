from pyspark.sql import functions as F

base = (
    df_shipments
    .filter((F.col("shipped_date") > F.to_date(F.lit("2023-12-22"))) & (F.col("carrier") != "Flash"))
    .select(
        "id",
        "order_id",
        "carrier",
        "cost",
        "status",
        F.dayofweek("delivered_date").alias("delivered_date_dayofweek"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("resolved_carrier"),
    )
)

result = (
    base
    .withColumn("cost_tier", F.when(F.col("cost") >= 96, "PREMIUM").otherwise("STANDARD"))
    .groupingSets([["cost_tier", "status"], ["cost_tier"], []], "cost_tier", "status")
    .agg(
        F.sum(F.when(F.col("carrier") == "Flash", F.col("delivered_date_dayofweek")).otherwise(0)).alias("flash_delivered_date_dayofweek"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.countDistinct("carrier").alias("distinct_carriers"),
        F.grouping("cost_tier").alias("grouping_id"),
    )
)
