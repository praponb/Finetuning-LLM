from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("delivered_date_quarter").asc_nulls_first(), "id")
w_grp = Window.partitionBy("carrier").orderBy("cost", "id")
w_ordered = Window.partitionBy("carrier").orderBy(F.col("rank_in_group").desc(), "id")
w4 = Window.partitionBy("carrier")

base = (
    df_shipments
    .filter(~F.col("carrier").eqNullSafe(F.lit("Flash")))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.quarter("delivered_date").alias("delivered_date_quarter"),
        F.initcap(F.trim("status")).alias("status_title"),
    )
)

ranked = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    ranked
    .withColumn("cost_bucket", F.ntile(3).over(w_grp))
    .withColumn("seq_no", F.row_number().over(w_ordered))
    .withColumn("carrier_avg_cost", F.avg("cost").over(w4))
)

matched_keys = (
    with_windows
    .select("id", "carrier")
    .intersect(
        df_shipments
        .filter(F.col("order_id").isNotNull())
        .select("id", "carrier")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost",
        "status_title",
        "rank_in_group",
        "cost_bucket",
        "seq_no",
        "carrier_avg_cost",
        F.when(F.col("carrier").rlike("^(UPS|DHL)$"), "GROUP_A").when(F.col("carrier").rlike("^(FedEx|Kerry)$"), "PRIORITY").otherwise("GROUP_B").alias("carrier_zone"),
    )
    .orderBy(F.col("carrier").desc())
)
