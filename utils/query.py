# File processed SQL -------------
file_agg = "SELECT DISTINCT filename FROM xdr_data"

# File processed SQL -------------
file_agg_recharge = "SELECT DISTINCT filename FROM recharge_data"

total_moc = """
SELECT
    served_imsi AS msisdn,
    COUNT(*) AS moc_count
FROM xdr_data
WHERE xdr_type = 'MOC'
GROUP BY served_imsi
"""
