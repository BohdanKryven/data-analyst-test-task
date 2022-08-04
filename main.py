import os
import pprint

import pandas as pd

from datetime import timedelta

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.management.collections import CollectionManager
from couchbase.options import (
    ClusterOptions,
    ClusterTimeoutOptions,
    QueryOptions
)
from couchbase.exceptions import (
    MissingConnectionException,
    AmbiguousTimeoutException,
    QueryIndexNotFoundException,
    CouchbaseException
)
from couchbase.collection import Collection

from handle_exceptions import handle_exception

endpoint = "cb.kx-fpmfl9w8dlm-h.cloud.couchbase.com"
username = "admin"
password = "Admin_Modema1"
bucket_name = "travel-sample"


def cluster_():
    auth = PasswordAuthenticator(username, password)
    timeout_opts = ClusterTimeoutOptions(kv_timeout=timedelta(seconds=10))
    cluster = Cluster('couchbases://{}'.format(endpoint),
                      ClusterOptions(auth, timeout_options=timeout_opts))
    cluster.wait_until_ready(timedelta(seconds=5))

    return cluster


def get_scopes():
    cluster = cluster_()

    connection = cluster.connection

    manager = CollectionManager(
        connection=connection,
        bucket_name=bucket_name
    )

    scopes = manager.get_all_scopes()
    return scopes


def create_primary_index(scope, collection):
    cluster = cluster_()

    try:
        primary_index = f"CREATE PRIMARY INDEX ON `default`:`{bucket_name}`.{scope}.{collection}"
        creating_primary_index = cluster.query(
            primary_index
        )
        creating_primary_index.execute()
    except CouchbaseException as ex:
        handle_exception(ex)


def add_column_collection(scope, collection):
    cluster = cluster_()

    test_column = f"UPDATE`{bucket_name}`.{scope}.{collection} SET testColumn = 'testData';"
    add_test_column = cluster.query(
        test_column,
    )
    add_test_column.execute()


def main():
    cluster = cluster_()
    scopes = get_scopes()

    for scope in scopes:
        collections = scope.collections
        print("Scope name: ", scope.name)

        for collection in collections:
            print("Collection name: ", collection.name)
            if not cluster.search_indexes():
                create_primary_index(scope=scope.name, collection=collection.name)

            sql_query = f"SELECT * FROM`{bucket_name}`.{scope.name}.{collection.name}"

            row_iter = cluster.query(
                sql_query,
                QueryOptions(metrics=True),
            )
            if row_iter:
                add_column_collection(scope=scope.name, collection=collection.name)

            data = []
            try:
                for row in row_iter.rows():
                    data.append(row[collection.name])

            except (
                    AmbiguousTimeoutException,
                    QueryIndexNotFoundException
            ) as ex:
                handle_exception(ex)
                continue

            os.makedirs(f"{bucket_name}/{scope.name}", exist_ok=True)
            df = pd.DataFrame(data)
            print(df.columns)
            df.to_csv(f"{bucket_name}/{scope.name}/{collection.name}.csv")


def merge_csv():
    pass


if __name__ == '__main__':
    main()
