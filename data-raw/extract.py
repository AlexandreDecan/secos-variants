import pandas
import subprocess
import os

# NOTE: This script requires a large quantity of memory. It is aimed to be
# executed on a computer with at least 32GB.

# The purpose of this script is to extract ecosystem-related data from libraries.io and
# to provide new csv files that will be ultimately easier to manage than the whole dump.
# As such, there is no guarantee that foreign keys are satisfied in the resulting .csv
# This has to be checked and/or taken into account by your own scripts.


# Selected ecosystem name (see libraries.io)
ECOSYSTEM = 'NPM'
# Version of the libraries.io dataset
LIBRARIESIO_VERSION = '1.6.0-2020-01-12'
# Location of the uncompressed libraries.io dataset
PATH_TO_LIBRARIESIO = '/data/libio1.6/'

# Mapping for "projects-[...].csv"
PROJECT_FIELDS = {
    'Platform': 'platform',
    'Name': 'package',
    'Licenses': 'license',
    'Repository URL': 'repository',
    'Repository ID': 'repoid',
}
# Mapping for "version-[...].csv"
VERSION_FIELDS = {
    'Platform': 'platform',
    'Project Name': 'package',
    'Number': 'version',
    'Published Timestamp': 'date',
}
# Mapping for "dependencies-[...].csv"
DEPENDENCY_FIELDS = {
    'Platform': 'platform',
    'Project Name': 'source',
    'Version Number': 'version',
    'Dependency Name': 'target',
    'Dependency Kind': 'kind',
    'Dependency Requirements': 'constraint',
    'Dependency Platform': 'target_platform'
}
# Kind of dependencies to keep
DEPENDENCY_KEPT_KINDS = ['normal', 'runtime', 'Development', 'development']
# Mapping for "repositories-[...].csv"
REPOSITORY_FIELDS = {
    'Host Type': 'host',
    'Name with Owner': 'repository',
    'ID': 'repoid',
    'Forks Count': 'forks',
    'Stars Count': 'stars',
    'Watchers Count': 'watchers',
    'Contributors Count': 'contributors',
    'License': 'license',
    'Fork Source Name with Owner': 'forked_from',
    'Created Timestamp': 'created_at',
    'Updated Timestamp': 'collected_at',
    'Last pushed Timestamp': 'last_push',
}
# Mapping for "repository_dependencies-[...].csv"
REPO_DEPENDENCY_FIELDS = {
    'Host Type': 'host',
    'Repository ID': 'repoid',
    'Repository Name with Owner': 'repository',
    'Manifest Platform': 'platform',
    'Dependency Project Name': 'target',
    'Dependency Kind': 'kind',
    'Dependency Requirements': 'constraint',
}
# Hosts to keep
REPO_KEPT_HOST = ['GitHub']
# Optimization purposes:
DATA_TYPES = {
    'host': 'category',
    'platform': 'category',
    'kind': 'category',
    'target_platform': 'category',
    'license': 'category',
    'target': 'category',
    'constraint': 'category',
    'repoid': 'Int64',
    'forks': 'Int32',
    'stars': 'Int32',
    'watchers': 'Int32',
    'contributors': 'Int32',
}

if __name__ == '__main__':
    print('Extracting data for {}, this could take some time...'.format(ECOSYSTEM))
    if not os.path.exists('temp-projects.csv'):
        with open('temp-projects.csv', 'w') as out:
            filename = os.path.join(PATH_TO_LIBRARIESIO, 'projects-{}.csv'.format(LIBRARIESIO_VERSION))
            subprocess.call(['head', '-1', filename], stdout=out)
            subprocess.call(['grep', ',{},'.format(ECOSYSTEM), filename], stdout=out)
        print('.. projects extracted')
    else:
        print('.. skipping projects')

    if not os.path.exists('temp-releases.csv'):
        with open('temp-releases.csv', 'w') as out:
            filename = os.path.join(PATH_TO_LIBRARIESIO, 'versions-{}.csv'.format(LIBRARIESIO_VERSION))
            subprocess.call(['head', '-1', filename], stdout=out)
            subprocess.call(['grep', ',{},'.format(ECOSYSTEM), filename], stdout=out)
        print('.. releases extracted')
    else:
        print('.. skipping releases')

    if not os.path.exists('temp-dependencies.csv'):
        with open('temp-dependencies.csv', 'w') as out:
            filename = os.path.join(PATH_TO_LIBRARIESIO, 'dependencies-{}.csv'.format(LIBRARIESIO_VERSION))
            subprocess.call(['head', '-1', filename], stdout=out)
            subprocess.call(['grep', ',{},'.format(ECOSYSTEM), filename], stdout=out)
        print('.. dependencies extracted')
    else:
        print('.. skipping dependencies')

    if not os.path.exists('temp-repo_deps.csv'):
        with open('temp-repo_deps.csv', 'w') as out:
            filename = os.path.join(PATH_TO_LIBRARIESIO, 'repository_dependencies-{}.csv'.format(LIBRARIESIO_VERSION))
            subprocess.call(['head', '-1', filename], stdout=out)
            subprocess.call(['grep', ',{},'.format(ECOSYSTEM), filename], stdout=out)
        print('.. repository dependencies extracted')
    else:
        print('.. skipping repository dependencies')



    print('Loading data in memory')
    df_packages = (
        pandas.read_csv(
            'temp-projects.csv',
            index_col=False,
            engine='c',
            low_memory=True,
            usecols=list(PROJECT_FIELDS.keys()),
            dtype={k: DATA_TYPES.get(v, 'object') for k, v in PROJECT_FIELDS.items()},
        )
        .rename(columns=PROJECT_FIELDS)
        .query('platform == "{}"'.format(ECOSYSTEM))
        .drop(columns=['platform'])
    )
    print('.. {} packages loaded'.format(len(df_packages)))

    df_releases = (
        pandas.read_csv(
            'temp-releases.csv',
            index_col=False,
            engine='c',
            low_memory=True,
            usecols=list(VERSION_FIELDS.keys()),
            dtype={k: DATA_TYPES.get(v, 'object') for k, v in VERSION_FIELDS.items()},
        )
        .rename(columns=VERSION_FIELDS)
        .query('platform == "{}"'.format(ECOSYSTEM))
        .drop(columns=['platform'])
    )
    print('.. {} releases loaded'.format(len(df_releases)))

    df_deps = (
        pandas.read_csv(
            'temp-dependencies.csv',
            index_col=False,
            engine='c',
            low_memory=True,
            usecols=list(DEPENDENCY_FIELDS.keys()),
            dtype={k: DATA_TYPES.get(v, 'object') for k, v in DEPENDENCY_FIELDS.items()},
        )
        .rename(columns=DEPENDENCY_FIELDS)
        .query('platform == "{0}" and target_platform == "{0}"'.format(ECOSYSTEM))
        .drop(columns=['platform', 'target_platform'])
    )
    print('.. {} dependencies loaded'.format(len(df_deps)))

    df_repo = (
        pandas.read_csv(
            os.path.join(PATH_TO_LIBRARIESIO, 'repositories-{}.csv'.format(LIBRARIESIO_VERSION)),
            index_col=False,
            engine='c',
            low_memory=True,
            usecols=list(REPOSITORY_FIELDS.keys()),
            dtype={k: DATA_TYPES.get(v, 'object') for k, v in REPOSITORY_FIELDS.items()},
        )
        .rename(columns=REPOSITORY_FIELDS)
    )
    print('.. {} repositories loaded'.format(len(df_repo)))

    df_repod = (
        pandas.read_csv(
            'temp-repo_deps.csv',
            index_col=False,
            engine='c',
            low_memory=True,
            usecols=list(REPO_DEPENDENCY_FIELDS.keys()),
            dtype={k: DATA_TYPES.get(v, 'object') for k, v in REPO_DEPENDENCY_FIELDS.items()},
        )
        .rename(columns=REPO_DEPENDENCY_FIELDS)
        .query('platform == "{}"'.format(ECOSYSTEM))
        .drop(columns=['platform'])
    )
    print('.. {} repository dependencies loaded'.format(len(df_repod)))



    print('Filtering repositories based on their host')
    df_repo = df_repo[lambda d: d['host'].isin(REPO_KEPT_HOST)]
    print('.. {} remaining repositories'.format(len(df_repo)))
    df_repod = df_repod[lambda d: d['host'].isin(REPO_KEPT_HOST)]
    print('.. {} remaining repository dependencies'.format(len(df_repod)))



    print('Filtering dependencies based on "kind"')
    df_deps = df_deps[lambda d: d['kind'].isin(DEPENDENCY_KEPT_KINDS)]
    print('.. {} remaining dependencies'.format(len(df_deps)))
    df_repod = df_repod[lambda d: d['kind'].isin(DEPENDENCY_KEPT_KINDS)]
    print('.. {} remaining repository dependencies'.format(len(df_repod)))



    print('Removing unknown packages')
    packages = df_packages['package'].drop_duplicates()
    print('.. {} known packages'.format(len(packages)))
    df_releases = df_releases[lambda d: d['package'].isin(packages)]
    print('.. {} remaining releases'.format(len(df_releases)))
    df_deps = df_deps[lambda d: d['source'].isin(packages)]
    df_deps = df_deps[lambda d: d['target'].isin(packages)]
    print('.. {} remaining dependencies'.format(len(df_deps)))
    df_repod = df_repod[lambda d: d['target'].isin(packages)]
    print('.. {} remaining repository dependencies'.format(len(df_repod)))



    print('Filtering repositories not related to the ecosystem')
    package_ids = df_packages['repoid'].drop_duplicates()
    project_ids = df_repod['repoid'].drop_duplicates()
    df_repo = df_repo[lambda d: d['repoid'].isin(package_ids.append(project_ids, ignore_index=True))]
    print('.. {} remaining repositories'.format(len(df_repo)))



    print('Exporting to compressed csv')
    df_packages[['package', 'license', 'repository', 'repoid']].to_csv(
        'packages.csv.gz',
        index=False,
        compression='gzip',
    )
    print('.. packages saved')

    df_releases[['package', 'version', 'date']].to_csv(
        'releases.csv.gz',
        index=False,
        compression='gzip',
    )
    print('.. releases saved')

    df_deps[['source', 'version', 'kind', 'target', 'constraint']].to_csv(
        'dependencies.csv.gz',
        index=False,
        compression='gzip',
    )
    print('.. dependencies saved')

    df_repo[['host', 'repository', 'repoid', 'forks', 'stars', 'watchers', 'contributors', 'license', 'forked_from', 'created_at', 'collected_at', 'last_push']].to_csv(
        'repositories.csv.gz',
        index=False,
        compression='gzip',
    )
    print('.. repositories saved')

    df_repod[['host', 'repository', 'repoid', 'kind', 'target', 'constraint']].to_csv(
        'repo_deps.csv.gz',
        index=False,
        compression='gzip',
    )
    print('.. repository dependencies saved')



    print('Deleting temporary files')
    subprocess.call(['rm', 'temp-projects.csv'])
    subprocess.call(['rm', 'temp-releases.csv'])
    subprocess.call(['rm', 'temp-dependencies.csv'])
    subprocess.call(['rm', 'temp-repo_deps.csv'])
    print()
