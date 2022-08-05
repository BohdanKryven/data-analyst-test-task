import os

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
    AmbiguousTimeoutException,
    QueryIndexNotFoundException,
    CouchbaseException
)

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


def get_scopes(cluster):
    connection = cluster.connection

    manager = CollectionManager(
        connection=connection,
        bucket_name=bucket_name
    )

    scopes = manager.get_all_scopes()
    return scopes


def create_primary_index(cluster, scope, collection):
    try:
        primary_index = f"CREATE PRIMARY INDEX ON `default`:`{bucket_name}`.{scope}.{collection}"
        creating_primary_index = cluster.query(
            primary_index
        )
        creating_primary_index.execute()
    except CouchbaseException as ex:
        handle_exception(ex)


def load_data(cluster, scope, collection):
    sql_query = f"SELECT * FROM`{bucket_name}`.{scope}.{collection}"

    row_iter = cluster.query(
        sql_query,
        QueryOptions(metrics=True),
    )
    if row_iter:
        add_column_collection(cluster=cluster, scope=scope, collection=collection)

    data = []
    try:
        for row in row_iter.rows():
            data.append(row[collection])

    except (
            AmbiguousTimeoutException,
            QueryIndexNotFoundException
    ) as ex:
        handle_exception(ex)

    return data


def add_column_collection(cluster, scope, collection):
    test_column = f"UPDATE`{bucket_name}`.{scope}.{collection} SET testColumn = 'testData';"
    add_test_column = cluster.query(
        test_column,
    )
    add_test_column.execute()


def merge_csv(df_local, scope, collection):
    df_db = pd.read_csv(f"{bucket_name}/{scope}/{collection}.csv")
    if "testColumn" not in df_local.columns and bool(list(df_local.columns)) is True:
        column_name = [column for column in df_local.columns if column in df_db.columns][0]

        df_local = df_local.merge(df_db, how="inner", on=column_name)
        df_local.to_csv(f"{bucket_name}/{scope}/{collection}.csv")


def save_to_csv(scope, collection, df):
    os.makedirs(f"{bucket_name}/{scope}", exist_ok=True)
    df.to_csv(f"{bucket_name}/{scope}/{collection}.csv")


def main():
    cluster = cluster_()
    scopes = get_scopes(cluster=cluster)

    for scope in scopes:
        collections = scope.collections
        scope_name = scope.name
        print("Scope name: ", scope_name)

        for collection in collections:
            collection_name = collection.name
            print("Collection name: ", collection_name)

            if not cluster.search_indexes():
                create_primary_index(cluster=cluster, scope=scope_name, collection=collection_name)

            add_column_collection(cluster=cluster, scope=scope_name, collection=collection_name)

            data = load_data(cluster=cluster, scope=scope_name, collection=collection_name)
            df = pd.DataFrame(data)

            save_to_csv(scope=scope_name, collection=collection_name, df=df)

            merge_csv(df_local=df, scope=scope_name, collection=collection_name)

        print("\n")


if __name__ == '__main__':
    main()
