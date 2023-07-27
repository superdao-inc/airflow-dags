import json
import logging
import subprocess
from time import sleep

import requests
from airflow.exceptions import AirflowSkipException

from common.clickhouse import exec_ch_driver_query


def get_block_from(blockchain) -> int:
    block_from = exec_ch_driver_query(
        f'SELECT max(number) FROM raw_data_{blockchain}.{blockchain}_etl_blocks'
    )[0][0]
    logging.info(f"block_from: {block_from}")
    return int(block_from)


def get_last_epoch() -> int:
    data = json.loads(requests.get('https://beaconcha.in/latestState').text)
    logging.info(f"CurrentState: {data}")
    return data['currentFinalizedEpoch']


def get_last_slot(epoch: int) -> int:
    return ((epoch + 1) * 32) - 1


def get_last_block(blockchain: str, slot: int) -> int:
    if blockchain == 'ethereum':
        r = requests.get(f'https://beaconcha.in/slot/{slot}').text
        block = r.find('<a href="/block/')
        res = r[block + 16 : block + 30]
        if res.find('"'):
            return int(res[: res.find('"')])
        else:
            return int(res)
    elif blockchain == 'polygon':
        response = json.loads(
            requests.post(
                'https://polygon-mainnet.g.alchemy.com/v2/GHDvgOcLW8-U_wwk5er-Lryn6jUjGz6V',
                json={"jsonrpc": "2.0", "id": 0, "method": "eth_blockNumber"},
            ).text
        )
        block = int(response['result'], 16)
        return block
    elif blockchain == 'optimism':
        response = json.loads(
            requests.post(
                'https://docs-demo.optimism.quiknode.pro/',
                json={"jsonrpc": "2.0", "id": 0, "method": "eth_blockNumber"},
            ).text
        )
        block = int(response['result'], 16)
        return block
    elif blockchain == 'arbitrum':
        response = json.loads(
            requests.post(
                'https://docs-demo.arbitrum-mainnet.quiknode.pro/',
                json={"jsonrpc": "2.0", "id": 0, "method": "eth_blockNumber"},
            ).text
        )
        block = int(response['result'], 16)
        return block
    elif blockchain == 'avalanche':
        response = json.loads(
            requests.post(
                'https://docs-demo.avalanche-mainnet.quiknode.pro/ext/bc/C/rpc/ext/bc/C/rpc',
                json={"jsonrpc": "2.0", "id": 0, "method": "eth_blockNumber"},
            ).text
        )
        block = int(response['result'], 16)
        return block


def get_etl_bucket_path(chain, data_type, start_block, end_block):
    return f'{chain}/{data_type}/{data_type}-{start_block}-{end_block}.csv.gz'


def gzip_csv(csv_path):
    subp = subprocess.run(f'gzip {csv_path}', shell=True, capture_output=True)
    logging.info(f"subprocess returncode: {subp.returncode}. stdout: {subp.stdout}")
    logging.error(f"subprocess stderr: {subp.stderr}")
    return csv_path + '.gz'


def unzip_csv(csv_path):
    subp = subprocess.run(f'gunzip {csv_path}', shell=True, capture_output=True)
    logging.info(f"subprocess returncode: {subp.returncode}. stdout: {subp.stdout}")
    logging.error(f"subprocess stderr: {subp.stderr}")
    return csv_path[:-3]


def ethereum_etl_prepare_data(**op_kwargs):
    batch_size = op_kwargs['batch_size'] if 'batch_size' in op_kwargs else 5000
    ti = op_kwargs['ti']
    blockchain = op_kwargs['blockchain']

    try:
        block_from = get_block_from(blockchain) + 1
        if block_from == 0 and blockchain == 'ethereum':
            block_from = 17124535
        elif block_from == 0 and blockchain == 'polygon':
            block_from = 42041293
        elif block_from == 0:
            block_from = 0
    except Exception as err:
        if blockchain == 'ethereum':
            block_from = 17124535
        elif blockchain == 'polygon':
            block_from = 42041293
        elif block_from == 0:
            block_from = 0

    block_to = (
        get_last_block(blockchain, get_last_slot(get_last_epoch()))
        if blockchain == 'ethereum'
        else get_last_block(blockchain, 0)
    )

    if block_to - block_from >= batch_size:
        block_to = block_from + batch_size
    # ti.xcom_push(key='block_from', value=block_from)
    # ti.xcom_push(key='block_to', value=block_to)

    logging.info(f"block_from: {block_from}, block_to: {block_to}")
    if block_to < block_from:
        raise AirflowSkipException
    else:
        return block_from, block_to


def truncate_buf_raw_data(**op_kwargs):
    table = op_kwargs['table']
    chain = op_kwargs['chain']

    query = f"""TRUNCATE TABLE {chain}_raw_data.buf_{table}"""
    res = exec_ch_driver_query(query)

    row_num = exec_ch_driver_query(
        f'SELECT count(1) as row_num FROM {chain}_raw_data.buf_{table}'
    )[0][0]

    while row_num:
        row_num = exec_ch_driver_query(
            f'SELECT count(1) as row_num FROM {chain}_raw_data.buf_{table}'
        )[0][0]
        sleep(5)

    print(f"""TRUNCATE TABLE {chain}_raw_data.buf_{table} OK!!!""")


def insert_raw_data_pipeline(**op_kwargs):
    table = op_kwargs['table']
    chain = op_kwargs['chain']
    raw_chain = {'ethereum': 'eth', 'polygon': 'polygon'}
    raw_chain = raw_chain[chain]

    if table == 'transactions':
        query = f"""INSERT INTO {chain}_raw_data.transactions
                SELECT distinct *
                FROM {chain}_raw_data.buf_transactions
                WHERE block_timestamp > (SELECT max(block_timestamp) FROM {chain}_raw_data.transactions)"""

        res = exec_ch_driver_query(query)
        logging.info(f"UPDATE transactions: {res}")

    elif table == 'blocks':
        query = f"""INSERT INTO {chain}_raw_data.blocks
                    SELECT distinct *
                    FROM {chain}_raw_data.buf_blocks
                    WHERE timestamp > (SELECT max(timestamp) FROM {chain}_raw_data.blocks)"""

        res = exec_ch_driver_query(query)
        logging.info(f"UPDATE blocks: {res}")

    elif table == 'contracts':
        query = f"""INSERT INTO {chain}_raw_data.contracts
                    SELECT distinct *
                    FROM {chain}_raw_data.buf_contracts
                    WHERE block_timestamp > (SELECT max(block_timestamp) FROM {chain}_raw_data.contracts)"""

        res = exec_ch_driver_query(query)
        logging.info(f"UPDATE contracts: {res}")

    elif table == 'logs':
        query = f"""INSERT INTO {chain}_raw_data.logs
                    SELECT distinct *
                    FROM {chain}_raw_data.buf_logs
                    WHERE block_timestamp > (SELECT max(block_timestamp) FROM {chain}_raw_data.logs)"""

        res = exec_ch_driver_query(query)
        logging.info(f"UPDATE logs: {res}")

    elif table == 'token_transfers':
        query = f"""INSERT INTO {chain}_raw_data.token_transfers
                    SELECT distinct *
                    FROM {chain}_raw_data.buf_token_transfers
                    WHERE block_timestamp > (SELECT max(block_timestamp) FROM {chain}_raw_data.token_transfers)"""

        res = exec_ch_driver_query(query)
        logging.info(f"UPDATE tokens: {res}")

    elif table == 'token_transfers':
        query = f"""INSERT INTO {chain}_raw_data.tokens
                    SELECT distinct *
                    FROM {chain}_raw_data.buf_tokens
                    WHERE block_timestamp > (SELECT max(block_timestamp) FROM {chain}_raw_data.tokens)"""

        res = exec_ch_driver_query(query)
        logging.info(f"UPDATE token_transfers: {res}")


def insert_raw_data_pipeline_old(**op_kwargs):
    table = op_kwargs['table']
    chain = op_kwargs['chain']
    raw_chain = {'ethereum': 'eth', 'polygon': 'polygon'}
    raw_chain = raw_chain[chain]

    if table == 'transactions':
        query = f"""INSERT INTO {raw_chain}_data.transactions_by_block (hash,transaction_index,from_address,to_address,value,gas_price,receipt_gas_used,block_number)
                SELECT distinct hash,
                        transaction_index,
                        from_address,
                        to_address,
                        value,
                        gas_price,
                        receipt_gas_used,
                        block_number
                FROM {chain}_raw_data.buf_transactions
                WHERE block_number > (SELECT max(block_number) FROM {raw_chain}_data.transactions_by_block)"""

        res = exec_ch_driver_query(query)
        logging.info(f"UPDATE transactions_by_block: {res}")

        query = f"""INSERT INTO {raw_chain}_data.transactions_by_from_address (hash,transaction_index,from_address,to_address,value,gas_price,receipt_gas_used,block_number)
                SELECT distinct hash,
                        transaction_index,
                        from_address,
                        to_address,
                        value,
                        gas_price,
                        receipt_gas_used,
                        block_number
                FROM {chain}_raw_data.buf_transactions
                WHERE block_number > (SELECT max(block_number) FROM {raw_chain}_data.transactions_by_from_address)"""

        res = exec_ch_driver_query(query)
        logging.info(f"UPDATE transactions_by_from_address: {res}")

        query = f"""INSERT INTO {raw_chain}_data.transactions_by_to_address (hash,transaction_index,from_address,to_address,value,gas_price,receipt_gas_used,block_number)
                SELECT distinct hash,
                        transaction_index,
                        from_address,
                        to_address,
                        value,
                        gas_price,
                        receipt_gas_used,
                        block_number
                FROM {chain}_raw_data.buf_transactions
                WHERE block_number > (SELECT max(block_number) FROM {raw_chain}_data.transactions_by_to_address)"""

        res = exec_ch_driver_query(query)
        logging.info(f"UPDATE transactions_by_to_address: {res}")

    elif table == 'blocks':
        query = f"""INSERT INTO {raw_chain}_data.blocks_by_number (number, hash, miner, gas_used, timestamp, base_fee_per_gas)
                    SELECT distinct number, hash, miner, gas_used, timestamp, base_fee_per_gas
                    FROM {chain}_raw_data.buf_blocks
                    WHERE number > (SELECT max(number) FROM {raw_chain}_data.blocks_by_number)"""

        res = exec_ch_driver_query(query)
        logging.info(f"UPDATE blocks_by_number: {res}")

    elif table == 'contracts_by_address':
        query = f"""INSERT INTO {raw_chain}_data.contracts_by_address (address, is_erc20, is_erc721)
                    SELECT distinct t1.address, 
                        t1.is_erc20, 
                        t1.is_erc721
                    FROM {chain}_raw_data.buf_contracts t1
                    LEFT JOIN {raw_chain}_data.contracts_by_address t2 on t1.address = t2.address
                    WHERE LENGTH(t2.address) = 0"""

        res = exec_ch_driver_query(query)
        logging.info(f"UPDATE contracts_by_address: {res}")

    elif table == 'receipts':
        pass

    elif table == 'traces':
        if chain == 'ethereum':
            query = f"""INSERT INTO {raw_chain}_data.traces_by_block (transaction_hash, from_address, to_address, value, call_type, status, block_number, trace_id)
                        SELECT distinct transaction_hash,
                                from_address,
                                to_address,
                                value,
                                call_type,
                                status,
                                block_number,
                                trace_id
                        FROM {chain}_raw_data.buf_traces
                        WHERE block_number > (SELECT max(block_number) FROM {raw_chain}_data.traces_by_block)"""

            res = exec_ch_driver_query(query)
            logging.info(f"UPDATE traces_by_block: {res}")

    elif table == 'token_transfers':
        query = f"""INSERT INTO {raw_chain}_data.token_transfers_by_from_address (token_address, from_address, to_address, value, transaction_hash, log_index, block_number)
                    SELECT distinct token_address,
                            from_address,
                            to_address,
                            value,
                            transaction_hash,
                            log_index,
                            block_number
                    FROM {chain}_raw_data.buf_token_transfers
                    WHERE block_number > (SELECT max(block_number) FROM {raw_chain}_data.token_transfers_by_from_address)"""

        res = exec_ch_driver_query(query)
        logging.info(f"UPDATE token_transfers_by_from_address: {res}")

        query = f"""INSERT INTO {raw_chain}_data.token_transfers_by_to_address (token_address, from_address, to_address, value, transaction_hash, log_index, block_number)
                    SELECT distinct token_address,
                            from_address,
                            to_address,
                            value,
                            transaction_hash,
                            log_index,
                            block_number
                    FROM {chain}_raw_data.buf_token_transfers
                    WHERE block_number > (SELECT max(block_number) FROM {raw_chain}_data.token_transfers_by_to_address)"""

        res = exec_ch_driver_query(query)
        logging.info(f"UPDATE token_transfers_by_to_address: {res}")

        query = f"""INSERT INTO {raw_chain}_data.token_transfers_by_token (token_address, from_address, to_address, value, transaction_hash, log_index, block_number)
                    SELECT distinct token_address,
                            from_address,
                            to_address,
                            value,
                            transaction_hash,
                            log_index,
                            block_number
                    FROM {chain}_raw_data.buf_token_transfers
                    WHERE block_number > (SELECT max(block_number) FROM {raw_chain}_data.token_transfers_by_token)"""

        res = exec_ch_driver_query(query)
        logging.info(f"UPDATE token_transfers_by_token: {res}")


def generate_dictionary():
    query = """CREATE or REPLACE TABLE eth_data.dictionary (key String, value Int256, value_type String, updated Datetime) ENGINE=MergeTree PRIMARY KEY (key)
                as
                SELECT 
                    'last_block_timestamp' as key,
                    max(timestamp) as value,
                    'INTEGER' as value_type,
                    now() as updated
                FROM raw_data_ethereum.ethereum_etl_blocks

                UNION ALL

                SELECT 
                    'eth_price_usd' as key,
                    toInt256(symbol_price) as value,
                    'INTEGER' as value_type,
                    now() as updated
                FROM (
                        SELECT 
                            symbol_price, 
                            ROW_NUMBER() OVER(PARTITION BY symbol_id ORDER BY load_timest desc) r
                        FROM raw_data.binance_prices WHERE symbol_id = 'ETHUSDT') 
                WHERE r = 1"""

    res = exec_ch_driver_query(query)
    logging.info(f"UPDATE eth_data.dictionary: {res}")
