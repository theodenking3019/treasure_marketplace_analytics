import requests
from ratelimit import limits, sleep_and_retry

ONE_SECOND = 1

@sleep_and_retry
@limits(calls=3, period=ONE_SECOND)
def get_contract_transactions(api_key, contract_address, from_address=None, tx_type="txlist"):
    """
     Abstracts a call to the Arbiscan API to get all transactions for a given contract.

     Args:
        api_key (str): your arbiscan API Key
        contract_address (str): contract address to query transactions for
        from_address: the address sending the transaction. Defaults to None, so all transactions from the passed
            contract are pulled.
        tx_type (str): the type of transactions to query. Either "txlist", "tokentx", or "tokennfttx".
            Defaults to "txlist"

    Returns:
        transactions (dict): dictionary of most recent 10k transactions for the passed contract
     """

    request_url = "https://api.arbiscan.io/api"
    request_url = request_url + "?module=account"
    request_url = request_url + "&action=" + tx_type
    request_url = request_url + "&address=" + (contract_address if from_address is None else from_address)
    if tx_type != "txlist":
        request_url = request_url + "&contractaddress=" + contract_address
    request_url = request_url + "&startblock=0"
    request_url = request_url + "&endblock=99999999"
    request_url = request_url + "&sort=desc"
    request_url = request_url + "&apikey=" + api_key

    response = requests.get(request_url)
    return response.json()