from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date")

base = (
    df_accounts
    .filter(F.col("opened_date") >= F.lit("2023-12-09").cast("date"))
    .withColumn("account_type_group", F.when(F.col("account_type").isin(["LOAN", "CREDIT"]), "Tier 1").otherwise("Other"))
    .withColumn("status_filled", F.coalesce("status", F.lit("DIRECT")))
)

ranked = (
    base
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

kept_keys = (
    ranked
    .select("customer_id")
    .exceptAll(
        df_accounts
        .filter(~F.col("account_type").isin(["CREDIT", "LOAN"]))
        .select("customer_id")
    )
)

reconciled = (
    ranked
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "account_type_group",
        "status_filled",
        F.when(F.col("account_type").rlike("^(CHECKING|CREDIT)$"), "PRIORITY").when(F.col("account_type").rlike("^(SAVINGS|LOAN)$"), "GROUP_A").otherwise("GROUP_B").alias("account_type_zone"),
    )
)
