
import influxdb_client, os, time
from datetime import datetime, date, timedelta, timezone

from influxdb_client import InfluxDBClient
import requests




def delete_DA_planning():
    bucket = "grid_data"
    org = "hevs"
    token = "F0cssT5LQoTFZXMZGiBxPU9CIZXFOM6S1xUnAnliDO3KHTHZ8_NgoLyPZvX6sEO60YznAEoAoSnZ6t5RwcLUjg=="
    url = "http://10.30.4.110:8086"

    client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)

    delete_api = client.delete_api()

    # Current time in UTC (RFC3339)
    now = datetime.now(timezone.utc).isoformat()

    delete_api.delete(
        start="2000-01-01T00:00:00Z",
        stop=now,
        predicate='_measurement="swissgrid_control_area_balance"',
        bucket=bucket,
        org=org
    )


delete_DA_planning()

