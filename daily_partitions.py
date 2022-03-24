from datetime import datetime as dt

import pandas as pd


def main():
    df = pd.read_csv('yellow-taxi-2015-01_mod.csv', nrows=100)
    df['tpep_pickup_datetime'] = df['tpep_pickup_datetime'].apply(lambda x: pd.Timestamp(x))

    daily_partitions = []

    for group in df.groupby(df['tpep_pickup_datetime'].apply(lambda x: dt.date(x))):
        daily_partitions.append(group[1])

    for daily_partition in daily_partitions:
        date_str = str(daily_partition.iloc[0]['tpep_pickup_datetime'])
        date_str = date_str.split()[0].replace('-', '_')
        daily_partition.to_csv(f'partitions/yellow_taxi_{date_str}.csv')


if __name__ == '__main__':
    main()
