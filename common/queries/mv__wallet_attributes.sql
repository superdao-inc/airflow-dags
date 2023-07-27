CREATE MATERIALIZED VIEW mv__wallet_attributes_tmp
        AS
        SELECT
            t.*,
            floor(
                least(
                    case
                    when t.nfts_count is null then
                        0
                    else
                        5.682 * least(ln(1 + nfts_count)/ ln(1 + 2), 4.4)
                    end +
                    case
                    when t.wallet_usd_cap is null then
                        0
                    else
                        1.72 * least(ln(1 + wallet_usd_cap)/ ln(1 + 1), 14)
                    end +
                    case
                    when t.created_at is null then
                        0
                    else
                        2.1 * least(ln(1 + extract(day from now() - created_at))/ ln(1 + 1), 10)
                    end +
                    case
                    when t.last_month_tx_count is null then
                        0
                    else
                        0.4 * least(ln(1 + last_month_tx_count)/ ln(1 + 0.13), 25)
                    end +
                    case
                    when t.twitter_followers_count is null then
                        0
                    else
                        6.25 * least(ln(1 + greatest(twitter_followers_count, 200))/ ln(1 + 200), 1.6)
                    end +
                    case
                    when t.ens_name is null then
                        0
                    else
                        10 * (ens_name is not null)::int
                    end,
                    100
                ) / (
                    case
                        when 'nonhuman' = any(t.labels) then 10
                        when 'hunter' = any(t.labels) then (1/0.6)
                        else 1
                    end
                )
            ) as superrank
        FROM (
            SELECT
                mv__all_wallets.address AS wallet,
                decode(substring(mv__all_wallets.address, 3), 'hex') AS wallet_b,

                LEAST(eth_attr_created_at.created_at, polygon_attr_created_at.created_at) AS created_at,

                off_chain_attr_ens_name.ens_name AS ens_name,
                off_chain_attr_twitter_avatar_url.twitter_avatar_url AS twitter_avatar_url,
                off_chain_attr_twitter_followers_count.twitter_followers_count AS twitter_followers_count,
                off_chain_attr_twitter_url.twitter_url AS twitter_url,
                off_chain_attr_twitter_username.twitter_username AS twitter_username,
                off_chain_attr_twitter_location.twitter_location AS twitter_location,
                off_chain_attr_twitter_bio.twitter_bio AS twitter_bio,

                off_chain_attr_email.email AS email,

                COALESCE(eth_attr_wallet_usd_cap.wallet_usd_cap, 0) + COALESCE(polygon_attr_wallet_usd_cap.wallet_usd_cap, 0) as wallet_usd_cap,

                CASE
                WHEN eth_attr_labels.labels IS NULL AND polygon_attr_labels.labels IS NULL THEN
                    NULL
                ELSE
                    array_unique(COALESCE(eth_attr_labels.labels, ARRAY[]::varchar[]) || COALESCE(polygon_attr_labels.labels, ARRAY[]::varchar[]))
                END as labels,

                CASE
                WHEN eth_attr_last_month_tx_count.last_month_tx_count IS NULL AND polygon_attr_last_month_tx_count.last_month_tx_count IS NULL THEN
                    NULL
                ELSE
                    COALESCE(eth_attr_last_month_tx_count.last_month_tx_count, 0) + COALESCE(polygon_attr_last_month_tx_count.last_month_tx_count, 0)
                END as last_month_tx_count,

                CASE
                WHEN eth_attr_last_month_in_volume.last_month_in_volume IS NULL AND polygon_attr_last_month_in_volume.last_month_in_volume IS NULL THEN
                    NULL
                ELSE
                    COALESCE(eth_attr_last_month_in_volume.last_month_in_volume, 0) + COALESCE(polygon_attr_last_month_in_volume.last_month_in_volume, 0)
                END as last_month_in_volume,

                CASE
                WHEN eth_attr_last_month_out_volume.last_month_out_volume IS NULL AND polygon_attr_last_month_out_volume.last_month_out_volume IS NULL THEN
                    NULL
                ELSE
                    COALESCE(eth_attr_last_month_out_volume.last_month_out_volume, 0) + COALESCE(polygon_attr_last_month_out_volume.last_month_out_volume, 0)
                END as last_month_out_volume,

                CASE
                WHEN eth_attr_last_month_in_volume.last_month_in_volume IS NULL AND eth_attr_last_month_out_volume.last_month_out_volume IS NULL AND polygon_attr_last_month_in_volume.last_month_in_volume IS NULL AND polygon_attr_last_month_out_volume.last_month_out_volume IS NULL THEN
                    NULL
                ELSE
                    COALESCE(eth_attr_last_month_in_volume.last_month_in_volume, 0) + COALESCE(eth_attr_last_month_out_volume.last_month_out_volume, 0) + COALESCE(polygon_attr_last_month_in_volume.last_month_in_volume, 0) + COALESCE(polygon_attr_last_month_out_volume.last_month_out_volume, 0)
                END as last_month_volume,

                CASE
                WHEN eth_attr_nfts_count.nfts_count IS NULL AND polygon_attr_nfts_count.nfts_count IS NULL THEN
                    NULL
                ELSE
                    COALESCE(eth_attr_nfts_count.nfts_count, 0) + COALESCE(polygon_attr_nfts_count.nfts_count, 0)
                END as nfts_count,

                CASE
                WHEN eth_attr_tx_count.tx_count IS NULL AND polygon_attr_tx_count.tx_count IS NULL THEN
                    NULL
                ELSE
                    COALESCE(eth_attr_tx_count.tx_count, 0) + COALESCE(polygon_attr_tx_count.tx_count, 0)
                END as tx_count,

                CASE
                WHEN eth_attr_whitelist_activity.whitelist_activity IS NULL AND polygon_attr_whitelist_activity.whitelist_activity IS NULL THEN
                    NULL
                ELSE
                    array_unique(COALESCE(eth_attr_whitelist_activity.whitelist_activity, ARRAY[]::varchar[]) || COALESCE(polygon_attr_whitelist_activity.whitelist_activity, ARRAY[]::varchar[]))
                END as whitelist_activity
                
            FROM mv__all_wallets
            LEFT OUTER JOIN eth_attr_created_at ON eth_attr_created_at.address = mv__all_wallets.address
            LEFT OUTER JOIN polygon_attr_created_at ON polygon_attr_created_at.address = mv__all_wallets.address

            LEFT OUTER JOIN off_chain_attr_ens_name ON off_chain_attr_ens_name.address = mv__all_wallets.address
            LEFT OUTER JOIN off_chain_attr_twitter_avatar_url ON off_chain_attr_twitter_avatar_url.address = mv__all_wallets.address
            LEFT OUTER JOIN off_chain_attr_twitter_followers_count ON off_chain_attr_twitter_followers_count.address = mv__all_wallets.address
            LEFT OUTER JOIN off_chain_attr_twitter_url ON off_chain_attr_twitter_url.address = mv__all_wallets.address
            LEFT OUTER JOIN off_chain_attr_twitter_username ON off_chain_attr_twitter_username.address = mv__all_wallets.address
            LEFT OUTER JOIN off_chain_attr_twitter_location ON off_chain_attr_twitter_location.address = mv__all_wallets.address
            LEFT OUTER JOIN off_chain_attr_twitter_bio ON off_chain_attr_twitter_bio.address = mv__all_wallets.address
            LEFT OUTER JOIN off_chain_attr_email ON off_chain_attr_email.address = mv__all_wallets.address

            LEFT OUTER JOIN eth_attr_labels ON eth_attr_labels.address = mv__all_wallets.address
            LEFT OUTER JOIN eth_attr_last_month_tx_count ON eth_attr_last_month_tx_count.address = mv__all_wallets.address
            LEFT OUTER JOIN eth_attr_last_month_in_volume ON eth_attr_last_month_in_volume.address = mv__all_wallets.address
            LEFT OUTER JOIN eth_attr_last_month_out_volume ON eth_attr_last_month_out_volume.address = mv__all_wallets.address
            LEFT OUTER JOIN eth_attr_nfts_count ON eth_attr_nfts_count.address = mv__all_wallets.address
            LEFT OUTER JOIN eth_attr_tx_count ON eth_attr_tx_count.address = mv__all_wallets.address
            LEFT OUTER JOIN eth_attr_wallet_usd_cap ON eth_attr_wallet_usd_cap.address = mv__all_wallets.address
            LEFT OUTER JOIN eth_attr_whitelist_activity ON eth_attr_whitelist_activity.address = mv__all_wallets.address

            LEFT OUTER JOIN polygon_attr_labels ON polygon_attr_labels.address = mv__all_wallets.address
            LEFT OUTER JOIN polygon_attr_last_month_tx_count ON polygon_attr_last_month_tx_count.address = mv__all_wallets.address
            LEFT OUTER JOIN polygon_attr_last_month_in_volume ON polygon_attr_last_month_in_volume.address = mv__all_wallets.address
            LEFT OUTER JOIN polygon_attr_last_month_out_volume ON polygon_attr_last_month_out_volume.address = mv__all_wallets.address
            LEFT OUTER JOIN polygon_attr_nfts_count ON polygon_attr_nfts_count.address = mv__all_wallets.address
            LEFT OUTER JOIN polygon_attr_tx_count ON polygon_attr_tx_count.address = mv__all_wallets.address
            LEFT OUTER JOIN polygon_attr_wallet_usd_cap ON polygon_attr_wallet_usd_cap.address = mv__all_wallets.address
            LEFT OUTER JOIN polygon_attr_whitelist_activity ON polygon_attr_whitelist_activity.address = mv__all_wallets.address
        ) as t;

-- create indices

CREATE INDEX ix_mv__wa_tmp__wallet_b ON mv__wallet_attributes_tmp (wallet_b);

CREATE INDEX ix_mv__wa_tmp__wallet ON mv__wallet_attributes_tmp (wallet);

CREATE INDEX "ix_mv__wa_tmp__column_1_ASC__label_1" ON mv__wallet_attributes_tmp (created_at ASC NULLS LAST, wallet_b) WHERE labels @> '{whale}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_DESC__label_1" ON mv__wallet_attributes_tmp (created_at DESC NULLS LAST, wallet_b) WHERE labels @> '{whale}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_ASC__label_1" ON mv__wallet_attributes_tmp (nfts_count ASC NULLS LAST, wallet_b) WHERE labels @> '{whale}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_DESC__label_1" ON mv__wallet_attributes_tmp (nfts_count DESC NULLS LAST, wallet_b) WHERE labels @> '{whale}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_ASC__label_1" ON mv__wallet_attributes_tmp (twitter_followers_count ASC NULLS LAST, wallet_b) WHERE labels @> '{whale}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_DESC__label_1" ON mv__wallet_attributes_tmp (twitter_followers_count DESC NULLS LAST, wallet_b) WHERE labels @> '{whale}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_ASC__label_1" ON mv__wallet_attributes_tmp (wallet_usd_cap ASC NULLS LAST, wallet_b) WHERE labels @> '{whale}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_DESC__label_1" ON mv__wallet_attributes_tmp (wallet_usd_cap DESC NULLS LAST, wallet_b) WHERE labels @> '{whale}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_ASC__label_1" ON mv__wallet_attributes_tmp (superrank ASC NULLS LAST, wallet_b) WHERE labels @> '{whale}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_DESC__label_1" ON mv__wallet_attributes_tmp (superrank DESC NULLS LAST, wallet_b) WHERE labels @> '{whale}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_ASC__label_1" ON mv__wallet_attributes_tmp (tx_count ASC NULLS LAST, wallet_b) WHERE labels @> '{whale}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_DESC__label_1" ON mv__wallet_attributes_tmp (tx_count DESC NULLS LAST, wallet_b) WHERE labels @> '{whale}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_ASC__label_2" ON mv__wallet_attributes_tmp (created_at ASC NULLS LAST, wallet_b) WHERE labels @> '{voter}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_DESC__label_2" ON mv__wallet_attributes_tmp (created_at DESC NULLS LAST, wallet_b) WHERE labels @> '{voter}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_ASC__label_2" ON mv__wallet_attributes_tmp (nfts_count ASC NULLS LAST, wallet_b) WHERE labels @> '{voter}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_DESC__label_2" ON mv__wallet_attributes_tmp (nfts_count DESC NULLS LAST, wallet_b) WHERE labels @> '{voter}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_ASC__label_2" ON mv__wallet_attributes_tmp (twitter_followers_count ASC NULLS LAST, wallet_b) WHERE labels @> '{voter}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_DESC__label_2" ON mv__wallet_attributes_tmp (twitter_followers_count DESC NULLS LAST, wallet_b) WHERE labels @> '{voter}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_ASC__label_2" ON mv__wallet_attributes_tmp (wallet_usd_cap ASC NULLS LAST, wallet_b) WHERE labels @> '{voter}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_DESC__label_2" ON mv__wallet_attributes_tmp (wallet_usd_cap DESC NULLS LAST, wallet_b) WHERE labels @> '{voter}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_ASC__label_2" ON mv__wallet_attributes_tmp (superrank ASC NULLS LAST, wallet_b) WHERE labels @> '{voter}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_DESC__label_2" ON mv__wallet_attributes_tmp (superrank DESC NULLS LAST, wallet_b) WHERE labels @> '{voter}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_ASC__label_2" ON mv__wallet_attributes_tmp (tx_count ASC NULLS LAST, wallet_b) WHERE labels @> '{voter}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_DESC__label_2" ON mv__wallet_attributes_tmp (tx_count DESC NULLS LAST, wallet_b) WHERE labels @> '{voter}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_ASC__label_3" ON mv__wallet_attributes_tmp (created_at ASC NULLS LAST, wallet_b) WHERE labels @> '{influencer}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_DESC__label_3" ON mv__wallet_attributes_tmp (created_at DESC NULLS LAST, wallet_b) WHERE labels @> '{influencer}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_ASC__label_3" ON mv__wallet_attributes_tmp (nfts_count ASC NULLS LAST, wallet_b) WHERE labels @> '{influencer}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_DESC__label_3" ON mv__wallet_attributes_tmp (nfts_count DESC NULLS LAST, wallet_b) WHERE labels @> '{influencer}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_ASC__label_3" ON mv__wallet_attributes_tmp (twitter_followers_count ASC NULLS LAST, wallet_b) WHERE labels @> '{influencer}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_DESC__label_3" ON mv__wallet_attributes_tmp (twitter_followers_count DESC NULLS LAST, wallet_b) WHERE labels @> '{influencer}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_ASC__label_3" ON mv__wallet_attributes_tmp (wallet_usd_cap ASC NULLS LAST, wallet_b) WHERE labels @> '{influencer}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_DESC__label_3" ON mv__wallet_attributes_tmp (wallet_usd_cap DESC NULLS LAST, wallet_b) WHERE labels @> '{influencer}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_ASC__label_3" ON mv__wallet_attributes_tmp (superrank ASC NULLS LAST, wallet_b) WHERE labels @> '{influencer}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_DESC__label_3" ON mv__wallet_attributes_tmp (superrank DESC NULLS LAST, wallet_b) WHERE labels @> '{influencer}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_ASC__label_3" ON mv__wallet_attributes_tmp (tx_count ASC NULLS LAST, wallet_b) WHERE labels @> '{influencer}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_DESC__label_3" ON mv__wallet_attributes_tmp (tx_count DESC NULLS LAST, wallet_b) WHERE labels @> '{influencer}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_ASC__label_4" ON mv__wallet_attributes_tmp (created_at ASC NULLS LAST, wallet_b) WHERE labels @> '{hunter}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_DESC__label_4" ON mv__wallet_attributes_tmp (created_at DESC NULLS LAST, wallet_b) WHERE labels @> '{hunter}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_ASC__label_4" ON mv__wallet_attributes_tmp (nfts_count ASC NULLS LAST, wallet_b) WHERE labels @> '{hunter}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_DESC__label_4" ON mv__wallet_attributes_tmp (nfts_count DESC NULLS LAST, wallet_b) WHERE labels @> '{hunter}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_ASC__label_4" ON mv__wallet_attributes_tmp (twitter_followers_count ASC NULLS LAST, wallet_b) WHERE labels @> '{hunter}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_DESC__label_4" ON mv__wallet_attributes_tmp (twitter_followers_count DESC NULLS LAST, wallet_b) WHERE labels @> '{hunter}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_ASC__label_4" ON mv__wallet_attributes_tmp (wallet_usd_cap ASC NULLS LAST, wallet_b) WHERE labels @> '{hunter}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_DESC__label_4" ON mv__wallet_attributes_tmp (wallet_usd_cap DESC NULLS LAST, wallet_b) WHERE labels @> '{hunter}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_ASC__label_4" ON mv__wallet_attributes_tmp (superrank ASC NULLS LAST, wallet_b) WHERE labels @> '{hunter}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_DESC__label_4" ON mv__wallet_attributes_tmp (superrank DESC NULLS LAST, wallet_b) WHERE labels @> '{hunter}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_ASC__label_4" ON mv__wallet_attributes_tmp (tx_count ASC NULLS LAST, wallet_b) WHERE labels @> '{hunter}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_DESC__label_4" ON mv__wallet_attributes_tmp (tx_count DESC NULLS LAST, wallet_b) WHERE labels @> '{hunter}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_ASC__label_5" ON mv__wallet_attributes_tmp (created_at ASC NULLS LAST, wallet_b) WHERE labels @> '{nonhuman}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_DESC__label_5" ON mv__wallet_attributes_tmp (created_at DESC NULLS LAST, wallet_b) WHERE labels @> '{nonhuman}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_ASC__label_5" ON mv__wallet_attributes_tmp (nfts_count ASC NULLS LAST, wallet_b) WHERE labels @> '{nonhuman}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_DESC__label_5" ON mv__wallet_attributes_tmp (nfts_count DESC NULLS LAST, wallet_b) WHERE labels @> '{nonhuman}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_ASC__label_5" ON mv__wallet_attributes_tmp (twitter_followers_count ASC NULLS LAST, wallet_b) WHERE labels @> '{nonhuman}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_DESC__label_5" ON mv__wallet_attributes_tmp (twitter_followers_count DESC NULLS LAST, wallet_b) WHERE labels @> '{nonhuman}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_ASC__label_5" ON mv__wallet_attributes_tmp (wallet_usd_cap ASC NULLS LAST, wallet_b) WHERE labels @> '{nonhuman}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_DESC__label_5" ON mv__wallet_attributes_tmp (wallet_usd_cap DESC NULLS LAST, wallet_b) WHERE labels @> '{nonhuman}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_ASC__label_5" ON mv__wallet_attributes_tmp (superrank ASC NULLS LAST, wallet_b) WHERE labels @> '{nonhuman}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_DESC__label_5" ON mv__wallet_attributes_tmp (superrank DESC NULLS LAST, wallet_b) WHERE labels @> '{nonhuman}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_ASC__label_5" ON mv__wallet_attributes_tmp (tx_count ASC NULLS LAST, wallet_b) WHERE labels @> '{nonhuman}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_DESC__label_5" ON mv__wallet_attributes_tmp (tx_count DESC NULLS LAST, wallet_b) WHERE labels @> '{nonhuman}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_ASC__label_6" ON mv__wallet_attributes_tmp (created_at ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:art}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_DESC__label_6" ON mv__wallet_attributes_tmp (created_at DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:art}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_ASC__label_6" ON mv__wallet_attributes_tmp (nfts_count ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:art}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_DESC__label_6" ON mv__wallet_attributes_tmp (nfts_count DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:art}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_ASC__label_6" ON mv__wallet_attributes_tmp (twitter_followers_count ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:art}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_DESC__label_6" ON mv__wallet_attributes_tmp (twitter_followers_count DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:art}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_ASC__label_6" ON mv__wallet_attributes_tmp (wallet_usd_cap ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:art}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_DESC__label_6" ON mv__wallet_attributes_tmp (wallet_usd_cap DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:art}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_ASC__label_6" ON mv__wallet_attributes_tmp (superrank ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:art}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_DESC__label_6" ON mv__wallet_attributes_tmp (superrank DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:art}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_ASC__label_6" ON mv__wallet_attributes_tmp (tx_count ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:art}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_DESC__label_6" ON mv__wallet_attributes_tmp (tx_count DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:art}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_ASC__label_7" ON mv__wallet_attributes_tmp (created_at ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:fasion}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_DESC__label_7" ON mv__wallet_attributes_tmp (created_at DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:fasion}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_ASC__label_7" ON mv__wallet_attributes_tmp (nfts_count ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:fasion}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_DESC__label_7" ON mv__wallet_attributes_tmp (nfts_count DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:fasion}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_ASC__label_7" ON mv__wallet_attributes_tmp (twitter_followers_count ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:fasion}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_DESC__label_7" ON mv__wallet_attributes_tmp (twitter_followers_count DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:fasion}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_ASC__label_7" ON mv__wallet_attributes_tmp (wallet_usd_cap ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:fasion}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_DESC__label_7" ON mv__wallet_attributes_tmp (wallet_usd_cap DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:fasion}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_ASC__label_7" ON mv__wallet_attributes_tmp (superrank ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:fasion}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_DESC__label_7" ON mv__wallet_attributes_tmp (superrank DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:fasion}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_ASC__label_7" ON mv__wallet_attributes_tmp (tx_count ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:fasion}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_DESC__label_7" ON mv__wallet_attributes_tmp (tx_count DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:fasion}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_ASC__label_8" ON mv__wallet_attributes_tmp (created_at ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:music}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_DESC__label_8" ON mv__wallet_attributes_tmp (created_at DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:music}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_ASC__label_8" ON mv__wallet_attributes_tmp (nfts_count ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:music}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_DESC__label_8" ON mv__wallet_attributes_tmp (nfts_count DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:music}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_ASC__label_8" ON mv__wallet_attributes_tmp (twitter_followers_count ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:music}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_DESC__label_8" ON mv__wallet_attributes_tmp (twitter_followers_count DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:music}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_ASC__label_8" ON mv__wallet_attributes_tmp (wallet_usd_cap ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:music}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_DESC__label_8" ON mv__wallet_attributes_tmp (wallet_usd_cap DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:music}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_ASC__label_8" ON mv__wallet_attributes_tmp (superrank ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:music}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_DESC__label_8" ON mv__wallet_attributes_tmp (superrank DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:music}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_ASC__label_8" ON mv__wallet_attributes_tmp (tx_count ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:music}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_DESC__label_8" ON mv__wallet_attributes_tmp (tx_count DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:music}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_ASC__label_9" ON mv__wallet_attributes_tmp (created_at ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:luxury}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_DESC__label_9" ON mv__wallet_attributes_tmp (created_at DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:luxury}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_ASC__label_9" ON mv__wallet_attributes_tmp (nfts_count ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:luxury}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_DESC__label_9" ON mv__wallet_attributes_tmp (nfts_count DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:luxury}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_ASC__label_9" ON mv__wallet_attributes_tmp (twitter_followers_count ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:luxury}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_DESC__label_9" ON mv__wallet_attributes_tmp (twitter_followers_count DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:luxury}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_ASC__label_9" ON mv__wallet_attributes_tmp (wallet_usd_cap ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:luxury}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_DESC__label_9" ON mv__wallet_attributes_tmp (wallet_usd_cap DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:luxury}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_ASC__label_9" ON mv__wallet_attributes_tmp (superrank ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:luxury}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_DESC__label_9" ON mv__wallet_attributes_tmp (superrank DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:luxury}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_ASC__label_9" ON mv__wallet_attributes_tmp (tx_count ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:luxury}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_DESC__label_9" ON mv__wallet_attributes_tmp (tx_count DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:culture:luxury}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_ASC__label_10" ON mv__wallet_attributes_tmp (created_at ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:defi}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_DESC__label_10" ON mv__wallet_attributes_tmp (created_at DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:defi}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_ASC__label_10" ON mv__wallet_attributes_tmp (nfts_count ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:defi}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_DESC__label_10" ON mv__wallet_attributes_tmp (nfts_count DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:defi}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_ASC__label_10" ON mv__wallet_attributes_tmp (twitter_followers_count ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:defi}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_DESC__label_10" ON mv__wallet_attributes_tmp (twitter_followers_count DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:defi}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_ASC__label_10" ON mv__wallet_attributes_tmp (wallet_usd_cap ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:defi}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_DESC__label_10" ON mv__wallet_attributes_tmp (wallet_usd_cap DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:defi}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_ASC__label_10" ON mv__wallet_attributes_tmp (superrank ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:defi}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_DESC__label_10" ON mv__wallet_attributes_tmp (superrank DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:defi}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_ASC__label_10" ON mv__wallet_attributes_tmp (tx_count ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:defi}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_DESC__label_10" ON mv__wallet_attributes_tmp (tx_count DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:defi}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_ASC__label_11" ON mv__wallet_attributes_tmp (created_at ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:developers}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_DESC__label_11" ON mv__wallet_attributes_tmp (created_at DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:developers}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_ASC__label_11" ON mv__wallet_attributes_tmp (nfts_count ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:developers}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_DESC__label_11" ON mv__wallet_attributes_tmp (nfts_count DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:developers}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_ASC__label_11" ON mv__wallet_attributes_tmp (twitter_followers_count ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:developers}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_DESC__label_11" ON mv__wallet_attributes_tmp (twitter_followers_count DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:developers}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_ASC__label_11" ON mv__wallet_attributes_tmp (wallet_usd_cap ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:developers}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_DESC__label_11" ON mv__wallet_attributes_tmp (wallet_usd_cap DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:developers}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_ASC__label_11" ON mv__wallet_attributes_tmp (superrank ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:developers}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_DESC__label_11" ON mv__wallet_attributes_tmp (superrank DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:developers}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_ASC__label_11" ON mv__wallet_attributes_tmp (tx_count ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:developers}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_DESC__label_11" ON mv__wallet_attributes_tmp (tx_count DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:developers}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_ASC__label_12" ON mv__wallet_attributes_tmp (created_at ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:donor}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_DESC__label_12" ON mv__wallet_attributes_tmp (created_at DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:donor}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_ASC__label_12" ON mv__wallet_attributes_tmp (nfts_count ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:donor}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_DESC__label_12" ON mv__wallet_attributes_tmp (nfts_count DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:donor}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_ASC__label_12" ON mv__wallet_attributes_tmp (twitter_followers_count ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:donor}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_DESC__label_12" ON mv__wallet_attributes_tmp (twitter_followers_count DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:donor}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_ASC__label_12" ON mv__wallet_attributes_tmp (wallet_usd_cap ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:donor}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_DESC__label_12" ON mv__wallet_attributes_tmp (wallet_usd_cap DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:donor}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_ASC__label_12" ON mv__wallet_attributes_tmp (superrank ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:donor}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_DESC__label_12" ON mv__wallet_attributes_tmp (superrank DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:donor}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_ASC__label_12" ON mv__wallet_attributes_tmp (tx_count ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:donor}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_DESC__label_12" ON mv__wallet_attributes_tmp (tx_count DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:donor}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_ASC__label_13" ON mv__wallet_attributes_tmp (created_at ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:early_adopters}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_DESC__label_13" ON mv__wallet_attributes_tmp (created_at DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:early_adopters}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_ASC__label_13" ON mv__wallet_attributes_tmp (nfts_count ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:early_adopters}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_DESC__label_13" ON mv__wallet_attributes_tmp (nfts_count DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:early_adopters}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_ASC__label_13" ON mv__wallet_attributes_tmp (twitter_followers_count ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:early_adopters}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_DESC__label_13" ON mv__wallet_attributes_tmp (twitter_followers_count DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:early_adopters}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_ASC__label_13" ON mv__wallet_attributes_tmp (wallet_usd_cap ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:early_adopters}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_DESC__label_13" ON mv__wallet_attributes_tmp (wallet_usd_cap DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:early_adopters}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_ASC__label_13" ON mv__wallet_attributes_tmp (superrank ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:early_adopters}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_DESC__label_13" ON mv__wallet_attributes_tmp (superrank DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:early_adopters}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_ASC__label_13" ON mv__wallet_attributes_tmp (tx_count ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:early_adopters}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_DESC__label_13" ON mv__wallet_attributes_tmp (tx_count DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:early_adopters}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_ASC__label_14" ON mv__wallet_attributes_tmp (created_at ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:gaming}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_DESC__label_14" ON mv__wallet_attributes_tmp (created_at DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:gaming}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_ASC__label_14" ON mv__wallet_attributes_tmp (nfts_count ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:gaming}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_DESC__label_14" ON mv__wallet_attributes_tmp (nfts_count DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:gaming}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_ASC__label_14" ON mv__wallet_attributes_tmp (twitter_followers_count ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:gaming}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_DESC__label_14" ON mv__wallet_attributes_tmp (twitter_followers_count DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:gaming}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_ASC__label_14" ON mv__wallet_attributes_tmp (wallet_usd_cap ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:gaming}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_DESC__label_14" ON mv__wallet_attributes_tmp (wallet_usd_cap DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:gaming}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_ASC__label_14" ON mv__wallet_attributes_tmp (superrank ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:gaming}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_DESC__label_14" ON mv__wallet_attributes_tmp (superrank DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:gaming}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_ASC__label_14" ON mv__wallet_attributes_tmp (tx_count ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:gaming}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_DESC__label_14" ON mv__wallet_attributes_tmp (tx_count DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:gaming}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_ASC__label_15" ON mv__wallet_attributes_tmp (created_at ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:investor}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_DESC__label_15" ON mv__wallet_attributes_tmp (created_at DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:investor}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_ASC__label_15" ON mv__wallet_attributes_tmp (nfts_count ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:investor}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_DESC__label_15" ON mv__wallet_attributes_tmp (nfts_count DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:investor}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_ASC__label_15" ON mv__wallet_attributes_tmp (twitter_followers_count ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:investor}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_DESC__label_15" ON mv__wallet_attributes_tmp (twitter_followers_count DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:investor}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_ASC__label_15" ON mv__wallet_attributes_tmp (wallet_usd_cap ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:investor}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_DESC__label_15" ON mv__wallet_attributes_tmp (wallet_usd_cap DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:investor}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_ASC__label_15" ON mv__wallet_attributes_tmp (superrank ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:investor}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_DESC__label_15" ON mv__wallet_attributes_tmp (superrank DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:investor}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_ASC__label_15" ON mv__wallet_attributes_tmp (tx_count ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:investor}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_DESC__label_15" ON mv__wallet_attributes_tmp (tx_count DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:investor}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_ASC__label_16" ON mv__wallet_attributes_tmp (created_at ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:professional}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_DESC__label_16" ON mv__wallet_attributes_tmp (created_at DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:professional}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_ASC__label_16" ON mv__wallet_attributes_tmp (nfts_count ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:professional}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_DESC__label_16" ON mv__wallet_attributes_tmp (nfts_count DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:professional}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_ASC__label_16" ON mv__wallet_attributes_tmp (twitter_followers_count ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:professional}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_DESC__label_16" ON mv__wallet_attributes_tmp (twitter_followers_count DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:professional}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_ASC__label_16" ON mv__wallet_attributes_tmp (wallet_usd_cap ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:professional}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_DESC__label_16" ON mv__wallet_attributes_tmp (wallet_usd_cap DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:professional}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_ASC__label_16" ON mv__wallet_attributes_tmp (superrank ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:professional}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_DESC__label_16" ON mv__wallet_attributes_tmp (superrank DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:professional}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_ASC__label_16" ON mv__wallet_attributes_tmp (tx_count ASC NULLS LAST, wallet_b) WHERE labels @> '{audience:professional}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_DESC__label_16" ON mv__wallet_attributes_tmp (tx_count DESC NULLS LAST, wallet_b) WHERE labels @> '{audience:professional}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_ASC__label_17" ON mv__wallet_attributes_tmp (created_at ASC NULLS LAST, wallet_b) WHERE labels @> '{nft_trader}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_DESC__label_17" ON mv__wallet_attributes_tmp (created_at DESC NULLS LAST, wallet_b) WHERE labels @> '{nft_trader}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_ASC__label_17" ON mv__wallet_attributes_tmp (nfts_count ASC NULLS LAST, wallet_b) WHERE labels @> '{nft_trader}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_DESC__label_17" ON mv__wallet_attributes_tmp (nfts_count DESC NULLS LAST, wallet_b) WHERE labels @> '{nft_trader}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_ASC__label_17" ON mv__wallet_attributes_tmp (twitter_followers_count ASC NULLS LAST, wallet_b) WHERE labels @> '{nft_trader}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_DESC__label_17" ON mv__wallet_attributes_tmp (twitter_followers_count DESC NULLS LAST, wallet_b) WHERE labels @> '{nft_trader}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_ASC__label_17" ON mv__wallet_attributes_tmp (wallet_usd_cap ASC NULLS LAST, wallet_b) WHERE labels @> '{nft_trader}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_DESC__label_17" ON mv__wallet_attributes_tmp (wallet_usd_cap DESC NULLS LAST, wallet_b) WHERE labels @> '{nft_trader}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_ASC__label_17" ON mv__wallet_attributes_tmp (superrank ASC NULLS LAST, wallet_b) WHERE labels @> '{nft_trader}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_DESC__label_17" ON mv__wallet_attributes_tmp (superrank DESC NULLS LAST, wallet_b) WHERE labels @> '{nft_trader}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_ASC__label_17" ON mv__wallet_attributes_tmp (tx_count ASC NULLS LAST, wallet_b) WHERE labels @> '{nft_trader}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_DESC__label_17" ON mv__wallet_attributes_tmp (tx_count DESC NULLS LAST, wallet_b) WHERE labels @> '{nft_trader}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_ASC__label_18" ON mv__wallet_attributes_tmp (created_at ASC NULLS LAST, wallet_b) WHERE labels @> '{passive}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_DESC__label_18" ON mv__wallet_attributes_tmp (created_at DESC NULLS LAST, wallet_b) WHERE labels @> '{passive}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_ASC__label_18" ON mv__wallet_attributes_tmp (nfts_count ASC NULLS LAST, wallet_b) WHERE labels @> '{passive}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_DESC__label_18" ON mv__wallet_attributes_tmp (nfts_count DESC NULLS LAST, wallet_b) WHERE labels @> '{passive}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_ASC__label_18" ON mv__wallet_attributes_tmp (twitter_followers_count ASC NULLS LAST, wallet_b) WHERE labels @> '{passive}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_DESC__label_18" ON mv__wallet_attributes_tmp (twitter_followers_count DESC NULLS LAST, wallet_b) WHERE labels @> '{passive}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_ASC__label_18" ON mv__wallet_attributes_tmp (wallet_usd_cap ASC NULLS LAST, wallet_b) WHERE labels @> '{passive}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_DESC__label_18" ON mv__wallet_attributes_tmp (wallet_usd_cap DESC NULLS LAST, wallet_b) WHERE labels @> '{passive}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_ASC__label_18" ON mv__wallet_attributes_tmp (superrank ASC NULLS LAST, wallet_b) WHERE labels @> '{passive}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_DESC__label_18" ON mv__wallet_attributes_tmp (superrank DESC NULLS LAST, wallet_b) WHERE labels @> '{passive}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_ASC__label_18" ON mv__wallet_attributes_tmp (tx_count ASC NULLS LAST, wallet_b) WHERE labels @> '{passive}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_DESC__label_18" ON mv__wallet_attributes_tmp (tx_count DESC NULLS LAST, wallet_b) WHERE labels @> '{passive}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_ASC__label_19" ON mv__wallet_attributes_tmp (created_at ASC NULLS LAST, wallet_b) WHERE labels @> '{zombie}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_DESC__label_19" ON mv__wallet_attributes_tmp (created_at DESC NULLS LAST, wallet_b) WHERE labels @> '{zombie}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_ASC__label_19" ON mv__wallet_attributes_tmp (nfts_count ASC NULLS LAST, wallet_b) WHERE labels @> '{zombie}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_2_DESC__label_19" ON mv__wallet_attributes_tmp (nfts_count DESC NULLS LAST, wallet_b) WHERE labels @> '{zombie}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_ASC__label_19" ON mv__wallet_attributes_tmp (twitter_followers_count ASC NULLS LAST, wallet_b) WHERE labels @> '{zombie}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_3_DESC__label_19" ON mv__wallet_attributes_tmp (twitter_followers_count DESC NULLS LAST, wallet_b) WHERE labels @> '{zombie}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_ASC__label_19" ON mv__wallet_attributes_tmp (wallet_usd_cap ASC NULLS LAST, wallet_b) WHERE labels @> '{zombie}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_4_DESC__label_19" ON mv__wallet_attributes_tmp (wallet_usd_cap DESC NULLS LAST, wallet_b) WHERE labels @> '{zombie}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_ASC__label_19" ON mv__wallet_attributes_tmp (superrank ASC NULLS LAST, wallet_b) WHERE labels @> '{zombie}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_5_DESC__label_19" ON mv__wallet_attributes_tmp (superrank DESC NULLS LAST, wallet_b) WHERE labels @> '{zombie}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_ASC__label_19" ON mv__wallet_attributes_tmp (tx_count ASC NULLS LAST, wallet_b) WHERE labels @> '{zombie}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_6_DESC__label_19" ON mv__wallet_attributes_tmp (tx_count DESC NULLS LAST, wallet_b) WHERE labels @> '{zombie}'::varchar[];;

CREATE INDEX "ix_mv__wa_tmp__column_1_ASC_wallet_b" ON mv__wallet_attributes_tmp (created_at ASC NULLS LAST, wallet_b);

CREATE INDEX "ix_mv__wa_tmp__column_1_DESC_wallet_b" ON mv__wallet_attributes_tmp (created_at DESC NULLS LAST, wallet_b);

CREATE INDEX "ix_mv__wa_tmp__column_2_ASC_wallet_b" ON mv__wallet_attributes_tmp (nfts_count ASC NULLS LAST, wallet_b);

CREATE INDEX "ix_mv__wa_tmp__column_2_DESC_wallet_b" ON mv__wallet_attributes_tmp (nfts_count DESC NULLS LAST, wallet_b);

CREATE INDEX "ix_mv__wa_tmp__column_3_ASC_wallet_b" ON mv__wallet_attributes_tmp (twitter_followers_count ASC NULLS LAST, wallet_b);

CREATE INDEX "ix_mv__wa_tmp__column_3_DESC_wallet_b" ON mv__wallet_attributes_tmp (twitter_followers_count DESC NULLS LAST, wallet_b);

CREATE INDEX "ix_mv__wa_tmp__column_4_ASC_wallet_b" ON mv__wallet_attributes_tmp (wallet_usd_cap ASC NULLS LAST, wallet_b);

CREATE INDEX "ix_mv__wa_tmp__column_4_DESC_wallet_b" ON mv__wallet_attributes_tmp (wallet_usd_cap DESC NULLS LAST, wallet_b);

CREATE INDEX "ix_mv__wa_tmp__column_5_ASC_wallet_b" ON mv__wallet_attributes_tmp (superrank ASC NULLS LAST, wallet_b);

CREATE INDEX "ix_mv__wa_tmp__column_5_DESC_wallet_b" ON mv__wallet_attributes_tmp (superrank DESC NULLS LAST, wallet_b);

CREATE INDEX "ix_mv__wa_tmp__column_6_ASC_wallet_b" ON mv__wallet_attributes_tmp (tx_count ASC NULLS LAST, wallet_b);

CREATE INDEX "ix_mv__wa_tmp__column_6_DESC_wallet_b" ON mv__wallet_attributes_tmp (tx_count DESC NULLS LAST, wallet_b);

CREATE INDEX ix_mv__wa_tmp__wallet_ens_name_lower ON mv__wallet_attributes_tmp (lower('ens_name'), lower('wallet'));

-- drop & rename: new -> old 

BEGIN;

DROP MATERIALIZED VIEW mv__wallet_attributes;

ALTER MATERIALIZED VIEW mv__wallet_attributes_tmp RENAME TO mv__wallet_attributes;

ALTER INDEX ix_mv__wa_tmp__wallet_b RENAME TO ix_mv__wa__wallet_b;

ALTER INDEX ix_mv__wa_tmp__wallet RENAME TO ix_mv__wa__wallet;

ALTER INDEX "ix_mv__wa_tmp__column_1_ASC__label_1" RENAME TO "ix_mv__wa__column_1_ASC__label_1";

ALTER INDEX "ix_mv__wa_tmp__column_1_DESC__label_1" RENAME TO "ix_mv__wa__column_1_DESC__label_1";

ALTER INDEX "ix_mv__wa_tmp__column_2_ASC__label_1" RENAME TO "ix_mv__wa__column_2_ASC__label_1";

ALTER INDEX "ix_mv__wa_tmp__column_2_DESC__label_1" RENAME TO "ix_mv__wa__column_2_DESC__label_1";

ALTER INDEX "ix_mv__wa_tmp__column_3_ASC__label_1" RENAME TO "ix_mv__wa__column_3_ASC__label_1";

ALTER INDEX "ix_mv__wa_tmp__column_3_DESC__label_1" RENAME TO "ix_mv__wa__column_3_DESC__label_1";

ALTER INDEX "ix_mv__wa_tmp__column_4_ASC__label_1" RENAME TO "ix_mv__wa__column_4_ASC__label_1";

ALTER INDEX "ix_mv__wa_tmp__column_4_DESC__label_1" RENAME TO "ix_mv__wa__column_4_DESC__label_1";

ALTER INDEX "ix_mv__wa_tmp__column_5_ASC__label_1" RENAME TO "ix_mv__wa__column_5_ASC__label_1";

ALTER INDEX "ix_mv__wa_tmp__column_5_DESC__label_1" RENAME TO "ix_mv__wa__column_5_DESC__label_1";

ALTER INDEX "ix_mv__wa_tmp__column_6_ASC__label_1" RENAME TO "ix_mv__wa__column_6_ASC__label_1";

ALTER INDEX "ix_mv__wa_tmp__column_6_DESC__label_1" RENAME TO "ix_mv__wa__column_6_DESC__label_1";

ALTER INDEX "ix_mv__wa_tmp__column_1_ASC__label_2" RENAME TO "ix_mv__wa__column_1_ASC__label_2";

ALTER INDEX "ix_mv__wa_tmp__column_1_DESC__label_2" RENAME TO "ix_mv__wa__column_1_DESC__label_2";

ALTER INDEX "ix_mv__wa_tmp__column_2_ASC__label_2" RENAME TO "ix_mv__wa__column_2_ASC__label_2";

ALTER INDEX "ix_mv__wa_tmp__column_2_DESC__label_2" RENAME TO "ix_mv__wa__column_2_DESC__label_2";

ALTER INDEX "ix_mv__wa_tmp__column_3_ASC__label_2" RENAME TO "ix_mv__wa__column_3_ASC__label_2";

ALTER INDEX "ix_mv__wa_tmp__column_3_DESC__label_2" RENAME TO "ix_mv__wa__column_3_DESC__label_2";

ALTER INDEX "ix_mv__wa_tmp__column_4_ASC__label_2" RENAME TO "ix_mv__wa__column_4_ASC__label_2";

ALTER INDEX "ix_mv__wa_tmp__column_4_DESC__label_2" RENAME TO "ix_mv__wa__column_4_DESC__label_2";

ALTER INDEX "ix_mv__wa_tmp__column_5_ASC__label_2" RENAME TO "ix_mv__wa__column_5_ASC__label_2";

ALTER INDEX "ix_mv__wa_tmp__column_5_DESC__label_2" RENAME TO "ix_mv__wa__column_5_DESC__label_2";

ALTER INDEX "ix_mv__wa_tmp__column_6_ASC__label_2" RENAME TO "ix_mv__wa__column_6_ASC__label_2";

ALTER INDEX "ix_mv__wa_tmp__column_6_DESC__label_2" RENAME TO "ix_mv__wa__column_6_DESC__label_2";

ALTER INDEX "ix_mv__wa_tmp__column_1_ASC__label_3" RENAME TO "ix_mv__wa__column_1_ASC__label_3";

ALTER INDEX "ix_mv__wa_tmp__column_1_DESC__label_3" RENAME TO "ix_mv__wa__column_1_DESC__label_3";

ALTER INDEX "ix_mv__wa_tmp__column_2_ASC__label_3" RENAME TO "ix_mv__wa__column_2_ASC__label_3";

ALTER INDEX "ix_mv__wa_tmp__column_2_DESC__label_3" RENAME TO "ix_mv__wa__column_2_DESC__label_3";

ALTER INDEX "ix_mv__wa_tmp__column_3_ASC__label_3" RENAME TO "ix_mv__wa__column_3_ASC__label_3";

ALTER INDEX "ix_mv__wa_tmp__column_3_DESC__label_3" RENAME TO "ix_mv__wa__column_3_DESC__label_3";

ALTER INDEX "ix_mv__wa_tmp__column_4_ASC__label_3" RENAME TO "ix_mv__wa__column_4_ASC__label_3";

ALTER INDEX "ix_mv__wa_tmp__column_4_DESC__label_3" RENAME TO "ix_mv__wa__column_4_DESC__label_3";

ALTER INDEX "ix_mv__wa_tmp__column_5_ASC__label_3" RENAME TO "ix_mv__wa__column_5_ASC__label_3";

ALTER INDEX "ix_mv__wa_tmp__column_5_DESC__label_3" RENAME TO "ix_mv__wa__column_5_DESC__label_3";

ALTER INDEX "ix_mv__wa_tmp__column_6_ASC__label_3" RENAME TO "ix_mv__wa__column_6_ASC__label_3";

ALTER INDEX "ix_mv__wa_tmp__column_6_DESC__label_3" RENAME TO "ix_mv__wa__column_6_DESC__label_3";

ALTER INDEX "ix_mv__wa_tmp__column_1_ASC__label_4" RENAME TO "ix_mv__wa__column_1_ASC__label_4";

ALTER INDEX "ix_mv__wa_tmp__column_1_DESC__label_4" RENAME TO "ix_mv__wa__column_1_DESC__label_4";

ALTER INDEX "ix_mv__wa_tmp__column_2_ASC__label_4" RENAME TO "ix_mv__wa__column_2_ASC__label_4";

ALTER INDEX "ix_mv__wa_tmp__column_2_DESC__label_4" RENAME TO "ix_mv__wa__column_2_DESC__label_4";

ALTER INDEX "ix_mv__wa_tmp__column_3_ASC__label_4" RENAME TO "ix_mv__wa__column_3_ASC__label_4";

ALTER INDEX "ix_mv__wa_tmp__column_3_DESC__label_4" RENAME TO "ix_mv__wa__column_3_DESC__label_4";

ALTER INDEX "ix_mv__wa_tmp__column_4_ASC__label_4" RENAME TO "ix_mv__wa__column_4_ASC__label_4";

ALTER INDEX "ix_mv__wa_tmp__column_4_DESC__label_4" RENAME TO "ix_mv__wa__column_4_DESC__label_4";

ALTER INDEX "ix_mv__wa_tmp__column_5_ASC__label_4" RENAME TO "ix_mv__wa__column_5_ASC__label_4";

ALTER INDEX "ix_mv__wa_tmp__column_5_DESC__label_4" RENAME TO "ix_mv__wa__column_5_DESC__label_4";

ALTER INDEX "ix_mv__wa_tmp__column_6_ASC__label_4" RENAME TO "ix_mv__wa__column_6_ASC__label_4";

ALTER INDEX "ix_mv__wa_tmp__column_6_DESC__label_4" RENAME TO "ix_mv__wa__column_6_DESC__label_4";

ALTER INDEX "ix_mv__wa_tmp__column_1_ASC__label_5" RENAME TO "ix_mv__wa__column_1_ASC__label_5";

ALTER INDEX "ix_mv__wa_tmp__column_1_DESC__label_5" RENAME TO "ix_mv__wa__column_1_DESC__label_5";

ALTER INDEX "ix_mv__wa_tmp__column_2_ASC__label_5" RENAME TO "ix_mv__wa__column_2_ASC__label_5";

ALTER INDEX "ix_mv__wa_tmp__column_2_DESC__label_5" RENAME TO "ix_mv__wa__column_2_DESC__label_5";

ALTER INDEX "ix_mv__wa_tmp__column_3_ASC__label_5" RENAME TO "ix_mv__wa__column_3_ASC__label_5";

ALTER INDEX "ix_mv__wa_tmp__column_3_DESC__label_5" RENAME TO "ix_mv__wa__column_3_DESC__label_5";

ALTER INDEX "ix_mv__wa_tmp__column_4_ASC__label_5" RENAME TO "ix_mv__wa__column_4_ASC__label_5";

ALTER INDEX "ix_mv__wa_tmp__column_4_DESC__label_5" RENAME TO "ix_mv__wa__column_4_DESC__label_5";

ALTER INDEX "ix_mv__wa_tmp__column_5_ASC__label_5" RENAME TO "ix_mv__wa__column_5_ASC__label_5";

ALTER INDEX "ix_mv__wa_tmp__column_5_DESC__label_5" RENAME TO "ix_mv__wa__column_5_DESC__label_5";

ALTER INDEX "ix_mv__wa_tmp__column_6_ASC__label_5" RENAME TO "ix_mv__wa__column_6_ASC__label_5";

ALTER INDEX "ix_mv__wa_tmp__column_6_DESC__label_5" RENAME TO "ix_mv__wa__column_6_DESC__label_5";

ALTER INDEX "ix_mv__wa_tmp__column_1_ASC__label_6" RENAME TO "ix_mv__wa__column_1_ASC__label_6";

ALTER INDEX "ix_mv__wa_tmp__column_1_DESC__label_6" RENAME TO "ix_mv__wa__column_1_DESC__label_6";

ALTER INDEX "ix_mv__wa_tmp__column_2_ASC__label_6" RENAME TO "ix_mv__wa__column_2_ASC__label_6";

ALTER INDEX "ix_mv__wa_tmp__column_2_DESC__label_6" RENAME TO "ix_mv__wa__column_2_DESC__label_6";

ALTER INDEX "ix_mv__wa_tmp__column_3_ASC__label_6" RENAME TO "ix_mv__wa__column_3_ASC__label_6";

ALTER INDEX "ix_mv__wa_tmp__column_3_DESC__label_6" RENAME TO "ix_mv__wa__column_3_DESC__label_6";

ALTER INDEX "ix_mv__wa_tmp__column_4_ASC__label_6" RENAME TO "ix_mv__wa__column_4_ASC__label_6";

ALTER INDEX "ix_mv__wa_tmp__column_4_DESC__label_6" RENAME TO "ix_mv__wa__column_4_DESC__label_6";

ALTER INDEX "ix_mv__wa_tmp__column_5_ASC__label_6" RENAME TO "ix_mv__wa__column_5_ASC__label_6";

ALTER INDEX "ix_mv__wa_tmp__column_5_DESC__label_6" RENAME TO "ix_mv__wa__column_5_DESC__label_6";

ALTER INDEX "ix_mv__wa_tmp__column_6_ASC__label_6" RENAME TO "ix_mv__wa__column_6_ASC__label_6";

ALTER INDEX "ix_mv__wa_tmp__column_6_DESC__label_6" RENAME TO "ix_mv__wa__column_6_DESC__label_6";

ALTER INDEX "ix_mv__wa_tmp__column_1_ASC__label_7" RENAME TO "ix_mv__wa__column_1_ASC__label_7";

ALTER INDEX "ix_mv__wa_tmp__column_1_DESC__label_7" RENAME TO "ix_mv__wa__column_1_DESC__label_7";

ALTER INDEX "ix_mv__wa_tmp__column_2_ASC__label_7" RENAME TO "ix_mv__wa__column_2_ASC__label_7";

ALTER INDEX "ix_mv__wa_tmp__column_2_DESC__label_7" RENAME TO "ix_mv__wa__column_2_DESC__label_7";

ALTER INDEX "ix_mv__wa_tmp__column_3_ASC__label_7" RENAME TO "ix_mv__wa__column_3_ASC__label_7";

ALTER INDEX "ix_mv__wa_tmp__column_3_DESC__label_7" RENAME TO "ix_mv__wa__column_3_DESC__label_7";

ALTER INDEX "ix_mv__wa_tmp__column_4_ASC__label_7" RENAME TO "ix_mv__wa__column_4_ASC__label_7";

ALTER INDEX "ix_mv__wa_tmp__column_4_DESC__label_7" RENAME TO "ix_mv__wa__column_4_DESC__label_7";

ALTER INDEX "ix_mv__wa_tmp__column_5_ASC__label_7" RENAME TO "ix_mv__wa__column_5_ASC__label_7";

ALTER INDEX "ix_mv__wa_tmp__column_5_DESC__label_7" RENAME TO "ix_mv__wa__column_5_DESC__label_7";

ALTER INDEX "ix_mv__wa_tmp__column_6_ASC__label_7" RENAME TO "ix_mv__wa__column_6_ASC__label_7";

ALTER INDEX "ix_mv__wa_tmp__column_6_DESC__label_7" RENAME TO "ix_mv__wa__column_6_DESC__label_7";

ALTER INDEX "ix_mv__wa_tmp__column_1_ASC__label_8" RENAME TO "ix_mv__wa__column_1_ASC__label_8";

ALTER INDEX "ix_mv__wa_tmp__column_1_DESC__label_8" RENAME TO "ix_mv__wa__column_1_DESC__label_8";

ALTER INDEX "ix_mv__wa_tmp__column_2_ASC__label_8" RENAME TO "ix_mv__wa__column_2_ASC__label_8";

ALTER INDEX "ix_mv__wa_tmp__column_2_DESC__label_8" RENAME TO "ix_mv__wa__column_2_DESC__label_8";

ALTER INDEX "ix_mv__wa_tmp__column_3_ASC__label_8" RENAME TO "ix_mv__wa__column_3_ASC__label_8";

ALTER INDEX "ix_mv__wa_tmp__column_3_DESC__label_8" RENAME TO "ix_mv__wa__column_3_DESC__label_8";

ALTER INDEX "ix_mv__wa_tmp__column_4_ASC__label_8" RENAME TO "ix_mv__wa__column_4_ASC__label_8";

ALTER INDEX "ix_mv__wa_tmp__column_4_DESC__label_8" RENAME TO "ix_mv__wa__column_4_DESC__label_8";

ALTER INDEX "ix_mv__wa_tmp__column_5_ASC__label_8" RENAME TO "ix_mv__wa__column_5_ASC__label_8";

ALTER INDEX "ix_mv__wa_tmp__column_5_DESC__label_8" RENAME TO "ix_mv__wa__column_5_DESC__label_8";

ALTER INDEX "ix_mv__wa_tmp__column_6_ASC__label_8" RENAME TO "ix_mv__wa__column_6_ASC__label_8";

ALTER INDEX "ix_mv__wa_tmp__column_6_DESC__label_8" RENAME TO "ix_mv__wa__column_6_DESC__label_8";

ALTER INDEX "ix_mv__wa_tmp__column_1_ASC__label_9" RENAME TO "ix_mv__wa__column_1_ASC__label_9";

ALTER INDEX "ix_mv__wa_tmp__column_1_DESC__label_9" RENAME TO "ix_mv__wa__column_1_DESC__label_9";

ALTER INDEX "ix_mv__wa_tmp__column_2_ASC__label_9" RENAME TO "ix_mv__wa__column_2_ASC__label_9";

ALTER INDEX "ix_mv__wa_tmp__column_2_DESC__label_9" RENAME TO "ix_mv__wa__column_2_DESC__label_9";

ALTER INDEX "ix_mv__wa_tmp__column_3_ASC__label_9" RENAME TO "ix_mv__wa__column_3_ASC__label_9";

ALTER INDEX "ix_mv__wa_tmp__column_3_DESC__label_9" RENAME TO "ix_mv__wa__column_3_DESC__label_9";

ALTER INDEX "ix_mv__wa_tmp__column_4_ASC__label_9" RENAME TO "ix_mv__wa__column_4_ASC__label_9";

ALTER INDEX "ix_mv__wa_tmp__column_4_DESC__label_9" RENAME TO "ix_mv__wa__column_4_DESC__label_9";

ALTER INDEX "ix_mv__wa_tmp__column_5_ASC__label_9" RENAME TO "ix_mv__wa__column_5_ASC__label_9";

ALTER INDEX "ix_mv__wa_tmp__column_5_DESC__label_9" RENAME TO "ix_mv__wa__column_5_DESC__label_9";

ALTER INDEX "ix_mv__wa_tmp__column_6_ASC__label_9" RENAME TO "ix_mv__wa__column_6_ASC__label_9";

ALTER INDEX "ix_mv__wa_tmp__column_6_DESC__label_9" RENAME TO "ix_mv__wa__column_6_DESC__label_9";

ALTER INDEX "ix_mv__wa_tmp__column_1_ASC__label_10" RENAME TO "ix_mv__wa__column_1_ASC__label_10";

ALTER INDEX "ix_mv__wa_tmp__column_1_DESC__label_10" RENAME TO "ix_mv__wa__column_1_DESC__label_10";

ALTER INDEX "ix_mv__wa_tmp__column_2_ASC__label_10" RENAME TO "ix_mv__wa__column_2_ASC__label_10";

ALTER INDEX "ix_mv__wa_tmp__column_2_DESC__label_10" RENAME TO "ix_mv__wa__column_2_DESC__label_10";

ALTER INDEX "ix_mv__wa_tmp__column_3_ASC__label_10" RENAME TO "ix_mv__wa__column_3_ASC__label_10";

ALTER INDEX "ix_mv__wa_tmp__column_3_DESC__label_10" RENAME TO "ix_mv__wa__column_3_DESC__label_10";

ALTER INDEX "ix_mv__wa_tmp__column_4_ASC__label_10" RENAME TO "ix_mv__wa__column_4_ASC__label_10";

ALTER INDEX "ix_mv__wa_tmp__column_4_DESC__label_10" RENAME TO "ix_mv__wa__column_4_DESC__label_10";

ALTER INDEX "ix_mv__wa_tmp__column_5_ASC__label_10" RENAME TO "ix_mv__wa__column_5_ASC__label_10";

ALTER INDEX "ix_mv__wa_tmp__column_5_DESC__label_10" RENAME TO "ix_mv__wa__column_5_DESC__label_10";

ALTER INDEX "ix_mv__wa_tmp__column_6_ASC__label_10" RENAME TO "ix_mv__wa__column_6_ASC__label_10";

ALTER INDEX "ix_mv__wa_tmp__column_6_DESC__label_10" RENAME TO "ix_mv__wa__column_6_DESC__label_10";

ALTER INDEX "ix_mv__wa_tmp__column_1_ASC__label_11" RENAME TO "ix_mv__wa__column_1_ASC__label_11";

ALTER INDEX "ix_mv__wa_tmp__column_1_DESC__label_11" RENAME TO "ix_mv__wa__column_1_DESC__label_11";

ALTER INDEX "ix_mv__wa_tmp__column_2_ASC__label_11" RENAME TO "ix_mv__wa__column_2_ASC__label_11";

ALTER INDEX "ix_mv__wa_tmp__column_2_DESC__label_11" RENAME TO "ix_mv__wa__column_2_DESC__label_11";

ALTER INDEX "ix_mv__wa_tmp__column_3_ASC__label_11" RENAME TO "ix_mv__wa__column_3_ASC__label_11";

ALTER INDEX "ix_mv__wa_tmp__column_3_DESC__label_11" RENAME TO "ix_mv__wa__column_3_DESC__label_11";

ALTER INDEX "ix_mv__wa_tmp__column_4_ASC__label_11" RENAME TO "ix_mv__wa__column_4_ASC__label_11";

ALTER INDEX "ix_mv__wa_tmp__column_4_DESC__label_11" RENAME TO "ix_mv__wa__column_4_DESC__label_11";

ALTER INDEX "ix_mv__wa_tmp__column_5_ASC__label_11" RENAME TO "ix_mv__wa__column_5_ASC__label_11";

ALTER INDEX "ix_mv__wa_tmp__column_5_DESC__label_11" RENAME TO "ix_mv__wa__column_5_DESC__label_11";

ALTER INDEX "ix_mv__wa_tmp__column_6_ASC__label_11" RENAME TO "ix_mv__wa__column_6_ASC__label_11";

ALTER INDEX "ix_mv__wa_tmp__column_6_DESC__label_11" RENAME TO "ix_mv__wa__column_6_DESC__label_11";

ALTER INDEX "ix_mv__wa_tmp__column_1_ASC__label_12" RENAME TO "ix_mv__wa__column_1_ASC__label_12";

ALTER INDEX "ix_mv__wa_tmp__column_1_DESC__label_12" RENAME TO "ix_mv__wa__column_1_DESC__label_12";

ALTER INDEX "ix_mv__wa_tmp__column_2_ASC__label_12" RENAME TO "ix_mv__wa__column_2_ASC__label_12";

ALTER INDEX "ix_mv__wa_tmp__column_2_DESC__label_12" RENAME TO "ix_mv__wa__column_2_DESC__label_12";

ALTER INDEX "ix_mv__wa_tmp__column_3_ASC__label_12" RENAME TO "ix_mv__wa__column_3_ASC__label_12";

ALTER INDEX "ix_mv__wa_tmp__column_3_DESC__label_12" RENAME TO "ix_mv__wa__column_3_DESC__label_12";

ALTER INDEX "ix_mv__wa_tmp__column_4_ASC__label_12" RENAME TO "ix_mv__wa__column_4_ASC__label_12";

ALTER INDEX "ix_mv__wa_tmp__column_4_DESC__label_12" RENAME TO "ix_mv__wa__column_4_DESC__label_12";

ALTER INDEX "ix_mv__wa_tmp__column_5_ASC__label_12" RENAME TO "ix_mv__wa__column_5_ASC__label_12";

ALTER INDEX "ix_mv__wa_tmp__column_5_DESC__label_12" RENAME TO "ix_mv__wa__column_5_DESC__label_12";

ALTER INDEX "ix_mv__wa_tmp__column_6_ASC__label_12" RENAME TO "ix_mv__wa__column_6_ASC__label_12";

ALTER INDEX "ix_mv__wa_tmp__column_6_DESC__label_12" RENAME TO "ix_mv__wa__column_6_DESC__label_12";

ALTER INDEX "ix_mv__wa_tmp__column_1_ASC__label_13" RENAME TO "ix_mv__wa__column_1_ASC__label_13";

ALTER INDEX "ix_mv__wa_tmp__column_1_DESC__label_13" RENAME TO "ix_mv__wa__column_1_DESC__label_13";

ALTER INDEX "ix_mv__wa_tmp__column_2_ASC__label_13" RENAME TO "ix_mv__wa__column_2_ASC__label_13";

ALTER INDEX "ix_mv__wa_tmp__column_2_DESC__label_13" RENAME TO "ix_mv__wa__column_2_DESC__label_13";

ALTER INDEX "ix_mv__wa_tmp__column_3_ASC__label_13" RENAME TO "ix_mv__wa__column_3_ASC__label_13";

ALTER INDEX "ix_mv__wa_tmp__column_3_DESC__label_13" RENAME TO "ix_mv__wa__column_3_DESC__label_13";

ALTER INDEX "ix_mv__wa_tmp__column_4_ASC__label_13" RENAME TO "ix_mv__wa__column_4_ASC__label_13";

ALTER INDEX "ix_mv__wa_tmp__column_4_DESC__label_13" RENAME TO "ix_mv__wa__column_4_DESC__label_13";

ALTER INDEX "ix_mv__wa_tmp__column_5_ASC__label_13" RENAME TO "ix_mv__wa__column_5_ASC__label_13";

ALTER INDEX "ix_mv__wa_tmp__column_5_DESC__label_13" RENAME TO "ix_mv__wa__column_5_DESC__label_13";

ALTER INDEX "ix_mv__wa_tmp__column_6_ASC__label_13" RENAME TO "ix_mv__wa__column_6_ASC__label_13";

ALTER INDEX "ix_mv__wa_tmp__column_6_DESC__label_13" RENAME TO "ix_mv__wa__column_6_DESC__label_13";

ALTER INDEX "ix_mv__wa_tmp__column_1_ASC__label_14" RENAME TO "ix_mv__wa__column_1_ASC__label_14";

ALTER INDEX "ix_mv__wa_tmp__column_1_DESC__label_14" RENAME TO "ix_mv__wa__column_1_DESC__label_14";

ALTER INDEX "ix_mv__wa_tmp__column_2_ASC__label_14" RENAME TO "ix_mv__wa__column_2_ASC__label_14";

ALTER INDEX "ix_mv__wa_tmp__column_2_DESC__label_14" RENAME TO "ix_mv__wa__column_2_DESC__label_14";

ALTER INDEX "ix_mv__wa_tmp__column_3_ASC__label_14" RENAME TO "ix_mv__wa__column_3_ASC__label_14";

ALTER INDEX "ix_mv__wa_tmp__column_3_DESC__label_14" RENAME TO "ix_mv__wa__column_3_DESC__label_14";

ALTER INDEX "ix_mv__wa_tmp__column_4_ASC__label_14" RENAME TO "ix_mv__wa__column_4_ASC__label_14";

ALTER INDEX "ix_mv__wa_tmp__column_4_DESC__label_14" RENAME TO "ix_mv__wa__column_4_DESC__label_14";

ALTER INDEX "ix_mv__wa_tmp__column_5_ASC__label_14" RENAME TO "ix_mv__wa__column_5_ASC__label_14";

ALTER INDEX "ix_mv__wa_tmp__column_5_DESC__label_14" RENAME TO "ix_mv__wa__column_5_DESC__label_14";

ALTER INDEX "ix_mv__wa_tmp__column_6_ASC__label_14" RENAME TO "ix_mv__wa__column_6_ASC__label_14";

ALTER INDEX "ix_mv__wa_tmp__column_6_DESC__label_14" RENAME TO "ix_mv__wa__column_6_DESC__label_14";

ALTER INDEX "ix_mv__wa_tmp__column_1_ASC__label_15" RENAME TO "ix_mv__wa__column_1_ASC__label_15";

ALTER INDEX "ix_mv__wa_tmp__column_1_DESC__label_15" RENAME TO "ix_mv__wa__column_1_DESC__label_15";

ALTER INDEX "ix_mv__wa_tmp__column_2_ASC__label_15" RENAME TO "ix_mv__wa__column_2_ASC__label_15";

ALTER INDEX "ix_mv__wa_tmp__column_2_DESC__label_15" RENAME TO "ix_mv__wa__column_2_DESC__label_15";

ALTER INDEX "ix_mv__wa_tmp__column_3_ASC__label_15" RENAME TO "ix_mv__wa__column_3_ASC__label_15";

ALTER INDEX "ix_mv__wa_tmp__column_3_DESC__label_15" RENAME TO "ix_mv__wa__column_3_DESC__label_15";

ALTER INDEX "ix_mv__wa_tmp__column_4_ASC__label_15" RENAME TO "ix_mv__wa__column_4_ASC__label_15";

ALTER INDEX "ix_mv__wa_tmp__column_4_DESC__label_15" RENAME TO "ix_mv__wa__column_4_DESC__label_15";

ALTER INDEX "ix_mv__wa_tmp__column_5_ASC__label_15" RENAME TO "ix_mv__wa__column_5_ASC__label_15";

ALTER INDEX "ix_mv__wa_tmp__column_5_DESC__label_15" RENAME TO "ix_mv__wa__column_5_DESC__label_15";

ALTER INDEX "ix_mv__wa_tmp__column_6_ASC__label_15" RENAME TO "ix_mv__wa__column_6_ASC__label_15";

ALTER INDEX "ix_mv__wa_tmp__column_6_DESC__label_15" RENAME TO "ix_mv__wa__column_6_DESC__label_15";

ALTER INDEX "ix_mv__wa_tmp__column_1_ASC__label_16" RENAME TO "ix_mv__wa__column_1_ASC__label_16";

ALTER INDEX "ix_mv__wa_tmp__column_1_DESC__label_16" RENAME TO "ix_mv__wa__column_1_DESC__label_16";

ALTER INDEX "ix_mv__wa_tmp__column_2_ASC__label_16" RENAME TO "ix_mv__wa__column_2_ASC__label_16";

ALTER INDEX "ix_mv__wa_tmp__column_2_DESC__label_16" RENAME TO "ix_mv__wa__column_2_DESC__label_16";

ALTER INDEX "ix_mv__wa_tmp__column_3_ASC__label_16" RENAME TO "ix_mv__wa__column_3_ASC__label_16";

ALTER INDEX "ix_mv__wa_tmp__column_3_DESC__label_16" RENAME TO "ix_mv__wa__column_3_DESC__label_16";

ALTER INDEX "ix_mv__wa_tmp__column_4_ASC__label_16" RENAME TO "ix_mv__wa__column_4_ASC__label_16";

ALTER INDEX "ix_mv__wa_tmp__column_4_DESC__label_16" RENAME TO "ix_mv__wa__column_4_DESC__label_16";

ALTER INDEX "ix_mv__wa_tmp__column_5_ASC__label_16" RENAME TO "ix_mv__wa__column_5_ASC__label_16";

ALTER INDEX "ix_mv__wa_tmp__column_5_DESC__label_16" RENAME TO "ix_mv__wa__column_5_DESC__label_16";

ALTER INDEX "ix_mv__wa_tmp__column_6_ASC__label_16" RENAME TO "ix_mv__wa__column_6_ASC__label_16";

ALTER INDEX "ix_mv__wa_tmp__column_6_DESC__label_16" RENAME TO "ix_mv__wa__column_6_DESC__label_16";

ALTER INDEX "ix_mv__wa_tmp__column_1_ASC__label_17" RENAME TO "ix_mv__wa__column_1_ASC__label_17";

ALTER INDEX "ix_mv__wa_tmp__column_1_DESC__label_17" RENAME TO "ix_mv__wa__column_1_DESC__label_17";

ALTER INDEX "ix_mv__wa_tmp__column_2_ASC__label_17" RENAME TO "ix_mv__wa__column_2_ASC__label_17";

ALTER INDEX "ix_mv__wa_tmp__column_2_DESC__label_17" RENAME TO "ix_mv__wa__column_2_DESC__label_17";

ALTER INDEX "ix_mv__wa_tmp__column_3_ASC__label_17" RENAME TO "ix_mv__wa__column_3_ASC__label_17";

ALTER INDEX "ix_mv__wa_tmp__column_3_DESC__label_17" RENAME TO "ix_mv__wa__column_3_DESC__label_17";

ALTER INDEX "ix_mv__wa_tmp__column_4_ASC__label_17" RENAME TO "ix_mv__wa__column_4_ASC__label_17";

ALTER INDEX "ix_mv__wa_tmp__column_4_DESC__label_17" RENAME TO "ix_mv__wa__column_4_DESC__label_17";

ALTER INDEX "ix_mv__wa_tmp__column_5_ASC__label_17" RENAME TO "ix_mv__wa__column_5_ASC__label_17";

ALTER INDEX "ix_mv__wa_tmp__column_5_DESC__label_17" RENAME TO "ix_mv__wa__column_5_DESC__label_17";

ALTER INDEX "ix_mv__wa_tmp__column_6_ASC__label_17" RENAME TO "ix_mv__wa__column_6_ASC__label_17";

ALTER INDEX "ix_mv__wa_tmp__column_6_DESC__label_17" RENAME TO "ix_mv__wa__column_6_DESC__label_17";

ALTER INDEX "ix_mv__wa_tmp__column_1_ASC__label_18" RENAME TO "ix_mv__wa__column_1_ASC__label_18";

ALTER INDEX "ix_mv__wa_tmp__column_1_DESC__label_18" RENAME TO "ix_mv__wa__column_1_DESC__label_18";

ALTER INDEX "ix_mv__wa_tmp__column_2_ASC__label_18" RENAME TO "ix_mv__wa__column_2_ASC__label_18";

ALTER INDEX "ix_mv__wa_tmp__column_2_DESC__label_18" RENAME TO "ix_mv__wa__column_2_DESC__label_18";

ALTER INDEX "ix_mv__wa_tmp__column_3_ASC__label_18" RENAME TO "ix_mv__wa__column_3_ASC__label_18";

ALTER INDEX "ix_mv__wa_tmp__column_3_DESC__label_18" RENAME TO "ix_mv__wa__column_3_DESC__label_18";

ALTER INDEX "ix_mv__wa_tmp__column_4_ASC__label_18" RENAME TO "ix_mv__wa__column_4_ASC__label_18";

ALTER INDEX "ix_mv__wa_tmp__column_4_DESC__label_18" RENAME TO "ix_mv__wa__column_4_DESC__label_18";

ALTER INDEX "ix_mv__wa_tmp__column_5_ASC__label_18" RENAME TO "ix_mv__wa__column_5_ASC__label_18";

ALTER INDEX "ix_mv__wa_tmp__column_5_DESC__label_18" RENAME TO "ix_mv__wa__column_5_DESC__label_18";

ALTER INDEX "ix_mv__wa_tmp__column_6_ASC__label_18" RENAME TO "ix_mv__wa__column_6_ASC__label_18";

ALTER INDEX "ix_mv__wa_tmp__column_6_DESC__label_18" RENAME TO "ix_mv__wa__column_6_DESC__label_18";

ALTER INDEX "ix_mv__wa_tmp__column_1_ASC__label_19" RENAME TO "ix_mv__wa__column_1_ASC__label_19";

ALTER INDEX "ix_mv__wa_tmp__column_1_DESC__label_19" RENAME TO "ix_mv__wa__column_1_DESC__label_19";

ALTER INDEX "ix_mv__wa_tmp__column_2_ASC__label_19" RENAME TO "ix_mv__wa__column_2_ASC__label_19";

ALTER INDEX "ix_mv__wa_tmp__column_2_DESC__label_19" RENAME TO "ix_mv__wa__column_2_DESC__label_19";

ALTER INDEX "ix_mv__wa_tmp__column_3_ASC__label_19" RENAME TO "ix_mv__wa__column_3_ASC__label_19";

ALTER INDEX "ix_mv__wa_tmp__column_3_DESC__label_19" RENAME TO "ix_mv__wa__column_3_DESC__label_19";

ALTER INDEX "ix_mv__wa_tmp__column_4_ASC__label_19" RENAME TO "ix_mv__wa__column_4_ASC__label_19";

ALTER INDEX "ix_mv__wa_tmp__column_4_DESC__label_19" RENAME TO "ix_mv__wa__column_4_DESC__label_19";

ALTER INDEX "ix_mv__wa_tmp__column_5_ASC__label_19" RENAME TO "ix_mv__wa__column_5_ASC__label_19";

ALTER INDEX "ix_mv__wa_tmp__column_5_DESC__label_19" RENAME TO "ix_mv__wa__column_5_DESC__label_19";

ALTER INDEX "ix_mv__wa_tmp__column_6_ASC__label_19" RENAME TO "ix_mv__wa__column_6_ASC__label_19";

ALTER INDEX "ix_mv__wa_tmp__column_6_DESC__label_19" RENAME TO "ix_mv__wa__column_6_DESC__label_19";

ALTER INDEX "ix_mv__wa_tmp__column_1_ASC_wallet_b" RENAME TO "ix_mv__wa__column_1_ASC_wallet_b";

ALTER INDEX "ix_mv__wa_tmp__column_1_DESC_wallet_b" RENAME TO "ix_mv__wa__column_1_DESC_wallet_b";

ALTER INDEX "ix_mv__wa_tmp__column_2_ASC_wallet_b" RENAME TO "ix_mv__wa__column_2_ASC_wallet_b";

ALTER INDEX "ix_mv__wa_tmp__column_2_DESC_wallet_b" RENAME TO "ix_mv__wa__column_2_DESC_wallet_b";

ALTER INDEX "ix_mv__wa_tmp__column_3_ASC_wallet_b" RENAME TO "ix_mv__wa__column_3_ASC_wallet_b";

ALTER INDEX "ix_mv__wa_tmp__column_3_DESC_wallet_b" RENAME TO "ix_mv__wa__column_3_DESC_wallet_b";

ALTER INDEX "ix_mv__wa_tmp__column_4_ASC_wallet_b" RENAME TO "ix_mv__wa__column_4_ASC_wallet_b";

ALTER INDEX "ix_mv__wa_tmp__column_4_DESC_wallet_b" RENAME TO "ix_mv__wa__column_4_DESC_wallet_b";

ALTER INDEX "ix_mv__wa_tmp__column_5_ASC_wallet_b" RENAME TO "ix_mv__wa__column_5_ASC_wallet_b";

ALTER INDEX "ix_mv__wa_tmp__column_5_DESC_wallet_b" RENAME TO "ix_mv__wa__column_5_DESC_wallet_b";

ALTER INDEX "ix_mv__wa_tmp__column_6_ASC_wallet_b" RENAME TO "ix_mv__wa__column_6_ASC_wallet_b";

ALTER INDEX "ix_mv__wa_tmp__column_6_DESC_wallet_b" RENAME TO "ix_mv__wa__column_6_DESC_wallet_b";

ALTER INDEX ix_mv__wa_tmp__wallet_ens_name_lower RENAME TO ix_mv__wa__wallet_ens_name_lower;

ALTER materialized VIEW mv__wallet_attributes OWNER TO scoring_api;
	
UPDATE alembic_version SET version_num='f94070f427e9' WHERE alembic_version.version_num = '0d04741cd516';

COMMIT;
