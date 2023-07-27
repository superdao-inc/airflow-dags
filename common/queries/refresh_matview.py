from airflow.exceptions import AirflowFailException
from tabulate import tabulate

from common.postgres import load_pg_driver_into_pandas


def test_all_attr():
    sql = """SELECT 
                'off_chain_attr_ens_name' as name,
                max(off_chain_attr_ens_name.updated), 
                DATE_PART('day', now()-max(off_chain_attr_ens_name.updated)) as date_diff,
                count(1)
            FROM off_chain_attr_ens_name

                UNION all
                
            SELECT 
                'off_chain_attr_twitter_avatar_url' as name,
                max(off_chain_attr_twitter_avatar_url.updated), 
                DATE_PART('day', now()-max(off_chain_attr_twitter_avatar_url.updated)) as date_diff,
                count(1)
            FROM off_chain_attr_twitter_avatar_url	

                UNION all
                
            SELECT 
                'off_chain_attr_twitter_followers_count' as name,
                max(off_chain_attr_twitter_followers_count.updated), 
                DATE_PART('day', now()-max(off_chain_attr_twitter_followers_count.updated)) as date_diff,
                count(1)
            FROM off_chain_attr_twitter_followers_count

                UNION all

            SELECT 
                'off_chain_attr_twitter_url' as name,
                max(off_chain_attr_twitter_url.updated), 
                DATE_PART('day', now()-max(off_chain_attr_twitter_url.updated)) as date_diff,
                count(1)
            FROM off_chain_attr_twitter_url

            UNION all

            SELECT 
                'off_chain_attr_twitter_username' as name,
                max(off_chain_attr_twitter_username.updated), 
                DATE_PART('day', now()-max(off_chain_attr_twitter_username.updated)) as date_diff,
                count(1)
            FROM off_chain_attr_twitter_username

            UNION all

            SELECT 
                'eth_attr_created_at' as name,
                max(eth_attr_created_at.updated),
                DATE_PART('day', now()-max(eth_attr_created_at.updated)) as date_diff,
                count(1)
            FROM eth_attr_created_at

            UNION all

            SELECT 
                'eth_attr_labels' as name,
                max(eth_attr_labels.updated), 
                DATE_PART('day', now()-max(eth_attr_labels.updated)) as date_diff,
                count(1)
            FROM eth_attr_labels

            UNION all

            SELECT 
                'eth_attr_last_month_tx_count' as name,
                max(eth_attr_last_month_tx_count.updated), 
                DATE_PART('day', now()-max(eth_attr_last_month_tx_count.updated)) as date_diff,
                count(1)
            FROM eth_attr_last_month_tx_count

            UNION all

            SELECT 
                'eth_attr_nfts_count' as name,
                max(eth_attr_nfts_count.updated), 
                DATE_PART('day', now()-max(eth_attr_nfts_count.updated)) as date_diff,
                count(1)
            FROM eth_attr_nfts_count

            UNION all

            select
                'eth_attr_tx_count' as name,
                max(eth_attr_tx_count.updated), 
                DATE_PART('day', now()-max(eth_attr_tx_count.updated)) as date_diff,
                count(1)
            FROM eth_attr_tx_count

            UNION all

            SELECT 
                'eth_attr_wallet_usd_cap' as name,
                max(eth_attr_wallet_usd_cap.updated), 
                DATE_PART('day', now()-max(eth_attr_wallet_usd_cap.updated)) as date_diff,
                count(1)
            FROM eth_attr_wallet_usd_cap

            UNION all

            SELECT 
                'eth_attr_whitelist_activity' as name,
                max(eth_attr_whitelist_activity.updated), 
                DATE_PART('day', now()-max(eth_attr_whitelist_activity.updated)) as date_diff,
                count(1)
            FROM eth_attr_whitelist_activity

            UNION all

            SELECT 
                'polygon_attr_created_at' as name,
                max(polygon_attr_created_at.updated), 
                DATE_PART('day', now()-max(polygon_attr_created_at.updated)) as date_diff,
                count(1)
            FROM polygon_attr_created_at

            UNION all

            SELECT 
                'polygon_attr_labels' as name,
                max(polygon_attr_labels.updated), 
                DATE_PART('day', now()-max(polygon_attr_labels.updated)) as date_diff,
                count(1)
            FROM polygon_attr_labels

            UNION all

            SELECT 
                'polygon_attr_last_month_tx_count' as name,
                max(polygon_attr_last_month_tx_count.updated), 
                DATE_PART('day', now()-max(polygon_attr_last_month_tx_count.updated)) as date_diff,
                count(1)
            FROM polygon_attr_last_month_tx_count

            UNION all

            SELECT 
                'polygon_attr_nfts_count' as name,
                max(polygon_attr_nfts_count.updated), 
                DATE_PART('day', now()-max(polygon_attr_nfts_count.updated)) as date_diff,
                count(1)
            FROM polygon_attr_nfts_count

            UNION all

            SELECT 
                'polygon_attr_tx_count' as name,
                max(polygon_attr_tx_count.updated), 
                DATE_PART('day', now()-max(polygon_attr_tx_count.updated)) as date_diff,
                count(1)
            FROM polygon_attr_tx_count

            UNION all

            SELECT 
                'polygon_attr_wallet_usd_cap' as name,
                max(polygon_attr_wallet_usd_cap.updated),
                DATE_PART('day', now()-max(polygon_attr_wallet_usd_cap.updated)) as date_diff,
                count(1)
            FROM polygon_attr_wallet_usd_cap

            UNION all

            SELECT 
                'polygon_attr_whitelist_activity' as name,
                max(polygon_attr_whitelist_activity.updated),
                DATE_PART('day', now()-max(polygon_attr_whitelist_activity.updated)) as date_diff,
                count(1)
            FROM polygon_attr_whitelist_activity"""

    test_data = load_pg_driver_into_pandas(sql)
    print(tabulate(test_data.values, headers=test_data.columns, tablefmt='orgtbl'))

    test_data.columns = ['name', 'updated', 'date_diff', 'count_rows']
    test_data['check_rows'] = [
        (0 if count_rows > 0 else 1) for count_rows in test_data['count_rows'].values
    ]

    if sum(test_data['check_rows']) == 0:  # sum(test_data['date_diff']) == 0 and
        pass
    else:
        raise AirflowFailException("Test data is FAIL!")
