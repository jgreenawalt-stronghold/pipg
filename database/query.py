sg_hashrate = """
WITH subaccounts AS (
	SELECT subaccount
	FROM capture.pool_accts
	WHERE site = 'Scrubgrass' AND active = 't'
)
SELECT
	sum(avg_15min_hashrate) / 10000000000000 as sg_hashrate
FROM capture.capt_pool_acct pool
RIGHT JOIN subaccounts ON subaccounts.subaccount = pool.acct
WHERE acct in (subaccounts.subaccount) AND timestamp >= now() - interval '10 minutes';"""

sg_foundry_jv_hashrate = """
SELECT avg_15min_hashrate / 1000000000000 as sg_foundry_jv_hashrate
FROM capture.capt_pool_acct
WHERE acct = 'btcpcreek2'
ORDER BY timestamp DESC LIMIT 1;"""

hashprice = "SELECT hashpriceusd FROM capture.capt_hashprice ORDER BY timestamp DESC LIMIT 1"
