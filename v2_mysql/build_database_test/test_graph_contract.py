import requests
import pandas as pd

query = """
{
  tokens(first: 1000, where: {category: Legion}, orderBy: tokenId) {
    name
    rarity
    tokenId
    metadata{
      __typename,
      ... on LegionInfo {
        id
        questing
        crafting
        role
      }
    }
  }
}
"""
legions_info = {}
request = requests.post('https://api.thegraph.com/subgraphs/name/treasureproject/bridgeworld'
                        '',
                        json={'query': query})
for data in request.json()['data']['tokens']:
    legions_info[data["tokenId"]] = {
        "generation": data["name"].split(' ')[0],
        "rarity": data["rarity"],
        "role": data["metadata"]["role"]
    }

legion_attributes = pd.DataFrame.from_dict(legions_info, orient="index").reset_index().rename(columns={'index':'id'})
print(legion_attributes.shape)






def main():
    last_id = 0

    while True:
        tokens = get_legions(last_id)

        if not tokens:
            break

        for token in tokens:
            if token["metadata"]:
                print(token["name"], token["tokenId"], token["metadata"]["role"])

        last_id = token["tokenId"]


def run_query(query):
    # endpoint where you are making the request
    request = requests.post('https://api.thegraph.com/subgraphs/name/treasureproject/bridgeworld'
                            '',
                            json={'query': query})
    if request.status_code == 200:
        return request.json()
    else:
        raise Exception('Query failed. return code is {}.      {}'.format(request.status_code, query))


def get_legions(token_id):
    with open("legions.gql", "r") as f:
        query = "".join(f.readlines())

        query = query.replace('$tokenId', f'"{token_id}"')
        res = run_query(query)

    return res["data"]["tokens"]

if __name__ == "__main__":
    main()

