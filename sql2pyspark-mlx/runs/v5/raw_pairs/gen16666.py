from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date", "id")
w2 = Window.partitionBy("carrier").orderBy(F.col("is_kerry").desc_nulls_first(), "id")
w3 = Window.partitionBy("status", "carrier").orderBy(F.col("is_kerry").desc(), "id")
w4 = Window.partitionBy("carrier", "status").orderBy(F.col("is_kerry").desc_nulls_first(), "id")

scored = (
    df_shipments
    .filter(F.col("delivered_date").between("2024-03-01", "2025-03-01"))
    .select(
        "id",
        "carrier",
        "cost",
        "status",
        F.lead("cost").over(w).alias("next_cost"),
        F.lag("cost").over(w).alias("prev_cost"),
        F.row_number().over(w).alias("seq_no"),
    )
)

base = (
    scored
    .filter(~F.col("carrier").isin(["DHL", "FedEx", "Flash"]) & F.col("carrier").isNotNull())
    .withColumn("seq_no_times_next_cost", (F.col("seq_no") * F.col("next_cost")))
    .withColumn("is_kerry", F.when(F.col("carrier") == "Kerry", 1).otherwise(0))
    .withColumn("resolved_carrier", F.coalesce("carrier", F.lit("DIRECT")))
)

ranked = (
    base
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

filtered = (
    ranked
    .filter(F.col("carrier").isNotNull())
    .select(
        "id",
        "carrier",
        "status",
        "next_cost",
        "prev_cost",
        "is_kerry",
        "resolved_carrier",
        F.col("seq_no").cast("string").alias("seq_no_str"),
        F.coalesce("carrier", F.lit("OTHER")).alias("carrier_clean"),
    )
)

ranked_shipments = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "carrier",
        "status",
        "prev_cost",
        "is_kerry",
        "resolved_carrier",
        "seq_no_str",
        "carrier_clean",
    )
)

deduped = (
    ranked_shipments
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

stats = (
    deduped
    .withColumn("is_kerry_tier", F.when(F.col("is_kerry") >= 60, "LARGE").otherwise("SMALL"))
    .groupingSets([["status", "is_kerry_tier"], ["status"]], "status", "is_kerry_tier")
    .agg(
        F.count("*").alias("event_count"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.count(F.when(F.col("carrier") == "DHL", 1)).alias("dhl_rows"),
        F.avg(F.when(F.col("status") == "DELIVERED", F.col("prev_cost"))).alias("delivered_avg_prev_cost"),
        F.sum(F.when(F.col("status") == "DELIVERED", F.col("is_kerry")).otherwise(0)).alias("delivered_is_kerry"),
    )
)

result = (
    stats
    .select(
        "status",
        "is_kerry_tier",
        "event_count",
        "carrier_list",
        "dhl_rows",
        "delivered_avg_prev_cost",
        "delivered_is_kerry",
        F.when(F.col("status").rlike("^(IN_TRANSIT|DELIVERED)$"), "GROUP_B").when(F.col("status").rlike("^(LOST|RETURNED)$"), "EXTENDED").otherwise("GROUP_A").alias("status_zone"),
    )
    .orderBy(F.col("status_zone").desc())
)
