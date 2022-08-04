import os
import pprint

import pandas as pd

from datetime import timedelta

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.management.collections import CollectionManager
from couchbase.options import ClusterOptions, ClusterTimeoutOptions, QueryOptions
from couchbase.exceptions import MissingConnectionException, AmbiguousTimeoutException, QueryIndexNotFoundException

endpoint = "cb.kx-fpmfl9w8dlm-h.cloud.couchbase.com"
username = "admin"
password = "Admin_Modema1"
bucket_name = "travel-sample"

auth = PasswordAuthenticator(username, password)
timeout_opts = ClusterTimeoutOptions(kv_timeout=timedelta(seconds=10))
cluster = Cluster('couchbases://{}'.format(endpoint),
                  ClusterOptions(auth, timeout_options=timeout_opts))
cluster.wait_until_ready(timedelta(seconds=5))


def main():
    if not cluster.connected:
        raise MissingConnectionException()

    connection = cluster.connection

    manager = CollectionManager(
        connection=connection,
        bucket_name=bucket_name
    )

    scopes = manager.get_all_scopes()
    print(scopes)

    for scope in scopes:
        collections = scope.collections
        print("Scope name: ", scope.name)

        for collection in collections:
            print("Collection name: ", collection.name)
            sql_query = f"SELECT * FROM `{bucket_name}`.{scope.name}.{collection.name}"

            row_iter = cluster.query(
                sql_query,
                QueryOptions(metrics=True)
            )
            res = []
            try:
                for row in row_iter.rows():
                    res.append(row[collection.name])

            except (
                    AmbiguousTimeoutException,
                    QueryIndexNotFoundException
            ) as ex:
                print(ex)
                continue

            os.makedirs(f"{bucket_name}/{scope.name}", exist_ok=True)
            df = pd.DataFrame(res)
            df.to_csv(f"{bucket_name}/{scope.name}/{collection.name}.csv")


if __name__ == '__main__':
    main()
