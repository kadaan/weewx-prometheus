#!/usr/local/weewx/.pyvenv/bin/python3

weather_metrics = {
    'outTemp': {'name': 'temp_degrees', 'id': 'Weather Station', 'type': 'gauge'},
    'outHumidity': {'name': 'humidity_percent', 'id': 'Weather Station', 'type': 'gauge'},
    'windDir': {'name': 'wind_speed_direction_degrees', 'id': 'Weather Station', 'type': 'gauge'},
    'windSpeed': {'name': 'wind_speed_mph', 'id': 'Weather Station', 'type': 'gauge'},
    'rain': {'name': 'rain_inches', 'id': 'Weather Station', 'type': 'gauge'},
    'outBattery': {'name': 'battery_ok', 'id': 'Weather Station', 'type': 'gauge'},
    'outSnr': {'name': 'snr', 'id': 'Weather Station', 'type': 'gauge'},
    'outRssi': {'name': 'rssi', 'id': 'Weather Station', 'type': 'gauge'},
    'outNoise': {'name': 'noise', 'id': 'Weather Station', 'type': 'gauge'},
    'pressure': {'name': 'pressure_hg', 'id': 'Weather Station', 'type': 'gauge'},
    'pm1_0': {'name': 'air_quality_1_0_ppm', 'id': 'Weather Station', 'type': 'gauge'},
    'pm2_5': {'name': 'air_quality_2_5_ppm', 'id': 'Weather Station', 'type': 'gauge'},
    'pm10_0': {'name': 'air_quality_10_0_ppm', 'id': 'Weather Station', 'type': 'gauge'},
    'pm2_5_aqi': {'name': 'air_quality_2_5_ppm_aqi', 'id': 'Weather Station', 'type': 'gauge'},
    'extraTemp1': {'name': 'temp_degrees', 'id': 'Caterpillar Tunnel 1', 'type': 'gauge'},
    'extraHumidity1': {'name': 'humidity_percent', 'id': 'Caterpillar Tunnel 1', 'type': 'gauge'},
    'extraBattery1': {'name': 'battery_ok', 'id': 'Caterpillar Tunnel 1', 'type': 'gauge'},
    'extraSnr1': {'name': 'snr', 'id': 'Caterpillar Tunnel 1', 'type': 'gauge'},
    'extraRssi1': {'name': 'rssi', 'id': 'Caterpillar Tunnel 1', 'type': 'gauge'},
    'extraNoise1': {'name': 'noise', 'id': 'Caterpillar Tunnel 1', 'type': 'gauge'},
    'extraTemp2': {'name': 'temp_degrees', 'id': 'Caterpillar Tunnel 3', 'type': 'gauge'},
    'extraHumidity2': {'name': 'humidity_percent', 'id': 'Caterpillar Tunnel 3', 'type': 'gauge'},
    'extraBattery2': {'name': 'battery_ok', 'id': 'Caterpillar Tunnel 3', 'type': 'gauge'},
    'extraSnr2': {'name': 'snr', 'id': 'Caterpillar Tunnel 3', 'type': 'gauge'},
    'extraRssi2': {'name': 'rssi', 'id': 'Caterpillar Tunnel 3', 'type': 'gauge'},
    'extraNoise2': {'name': 'noise', 'id': 'Caterpillar Tunnel 3', 'type': 'gauge'},
    'extraTemp3': {'name': 'temp_degrees', 'id': 'Workshop', 'type': 'gauge'},
    'extraHumidity3': {'name': 'humidity_percent', 'id': 'Workshop', 'type': 'gauge'},
    'extraBattery3': {'name': 'battery_ok', 'id': 'Workshop', 'type': 'gauge'},
    'extraSnr3': {'name': 'snr', 'id': 'Workshop', 'type': 'gauge'},
    'extraRssi3': {'name': 'rssi', 'id': 'Workshop', 'type': 'gauge'},
    'extraNoise3': {'name': 'noise', 'id': 'Workshop', 'type': 'gauge'},
    'extraTemp4': {'name': 'temp_degrees', 'id': 'Propagation Greenhouse', 'type': 'gauge'},
    'extraHumidity4': {'name': 'humidity_percent', 'id': 'Propagation Greenhouse', 'type': 'gauge'},
    'extraBattery4': {'name': 'battery_ok', 'id': 'Propagation Greenhouse', 'type': 'gauge'},
    'extraSnr4': {'name': 'snr', 'id': 'Propagation Greenhouse', 'type': 'gauge'},
    'extraRssi4': {'name': 'rssi', 'id': 'Propagation Greenhouse', 'type': 'gauge'},
    'extraNoise4': {'name': 'noise', 'id': 'Propagation Greenhouse', 'type': 'gauge'},
    'extraTemp5': {'name': 'temp_degrees', 'id': 'Farm Stand', 'type': 'gauge'},
    'extraHumidity5': {'name': 'humidity_percent', 'id': 'Farm Stand', 'type': 'gauge'},
    'extraBattery5': {'name': 'battery_ok', 'id': 'Farm Stand', 'type': 'gauge'},
    'extraSnr5': {'name': 'snr', 'id': 'Farm Stand', 'type': 'gauge'},
    'extraRssi5': {'name': 'rssi', 'id': 'Farm Stand', 'type': 'gauge'},
    'extraNoise5': {'name': 'noise', 'id': 'Farm Stand', 'type': 'gauge'},
    'extraTemp6': {'name': 'temp_degrees', 'id': 'Farm Stand Fridge', 'type': 'gauge'},
    'extraHumidity6': {'name': 'humidity_percent', 'id': 'Farm Stand Fridge', 'type': 'gauge'}
}

__version__ = '1.2.0'

import weewx
import weewx.restx
import weeutil.weeutil

import requests

import queue
import sys
import syslog

class PromPush(weewx.restx.StdRESTful):
    """

    sends weewx weather records to a prometheus pushgateway using the
    prometheus_client library

    """

    def __init__(self, engine, config_dict):
        super(PromPush, self).__init__(engine, config_dict)
        try:
            _prom_dict = weeutil.weeutil.accumulateLeaves(
                config_dict['StdRESTful']['PromPush'], max_level=1)
        except KeyError as e:
            logerr("config error: missing parameter %s" % e)
            return

        _manager_dict = weewx.manager.get_manager_dict(
            config_dict['DataBindings'], config_dict['Databases'], 'wx_binding')

        self.loop_queue = queue.Queue()
        self.loop_thread = PromPushThread(self.loop_queue, _manager_dict,
                                          **_prom_dict)
        self.loop_thread.start()
        self.bind(weewx.NEW_LOOP_PACKET, self.new_loop_packet)
        loginfo("data will be sent to pushgateway at %s:%s" %
                (_prom_dict['host'], _prom_dict['port']))

    def new_loop_packet(self, event):
        self.loop_queue.put(event.packet)


class PromPushThread(weewx.restx.RESTThread):
    """
    thread for sending data to the configured prometheus pushgateway
    """

    DEFAULT_HOST = 'localhost'
    DEFAULT_PORT = '9091'
    DEFAULT_JOB = 'weewx'
    DEFAULT_TIMEOUT = 10
    DEFAULT_MAX_TRIES = 3
    DEFAULT_RETRY_WAIT = 5

    def __init__(self, queue, manager_dict,
                 host=DEFAULT_HOST,
                 port=DEFAULT_PORT,
                 job=DEFAULT_JOB,
                 skip_post=False,
                 max_backlog=sys.maxsize,
                 stale=60,
                 log_success=True,
                 log_failure=True,
                 timeout=DEFAULT_TIMEOUT,
                 max_tries=DEFAULT_MAX_TRIES,
                 retry_wait=DEFAULT_RETRY_WAIT):


        super(PromPushThread, self).__init__(
            queue,
            protocol_name='PromPush',
            manager_dict=manager_dict,
            max_backlog=max_backlog,
            stale=stale,
            log_success=log_success,
            log_failure=log_failure,
            timeout=timeout,
            max_tries=max_tries,
            retry_wait=retry_wait
        )

        self.host = host
        self.port = port
        self.job = job
        self.skip_post = weeutil.weeutil.to_bool(skip_post)

    def post_metrics(self, instance, data):
        # print("---> %s\n%s\n" % (instance, data))
        # post the weather stats to the prometheus push gw
        pushgw_url = 'http://' + self.host + ":" + self.port + "/metrics/job/" + self.job + "/instance/" + instance

        try:
            _res = requests.post(url=pushgw_url,
                                data=data,
                                headers={'Content-Type': 'application/octet-stream'})
            if 200 <= _res.status_code <= 299:
                # success
                # logdbg("pushgw post return code - %s" % _res.status_code)
                return
            else:
                # something went awry
                logerr("pushgw post error: %s" % _res.text)
                return

        except requests.ConnectionError as e:
            logerr("pushgw post error: %s" % e.message)


    def process_record(self, record, dbm):
        _ = dbm

        if self.skip_post:
            loginfo("-- prompush: skipping post")
        else:
            metrics = {}
            dates = {}
            for key, val in record.items():
                if val is None:
                    val = 0.0

                if weather_metrics.get(key):
                    weather_metric = weather_metrics[key]
                    id = weather_metric['id']
                    name = weather_metric['name']
                    type = weather_metric['type']
                    value = str(val)
                    record_data = ''
                    if id in metrics:
                        record_data = metrics[id]
                    record_data += "# TYPE weewx_loop_%s %s\n" % (name, type)
                    record_data += "weewx_loop_%s %s\n" % (name, value)
                    metrics[id] = record_data
                    if "dateTime" in record:
                        date = record["dateTime"]
                        if id not in dates or dates[id] < date:
                            dates[id] = date
                else:
                    if key != 'type':
                        logdbg("missing field [%s] in defs" % (key))

            for key, value in dates.items():
                val = str(value)
                record_data = metrics[key]
                record_data += "# TYPE weewx_loop_last_processed_timestamp_seconds gauge\n"
                record_data += "weewx_loop_last_processed_timestamp_seconds %s\n" % (val)
                metrics[key] = record_data

            for key, value in metrics.items():
                self.post_metrics(key, value)


#---------------------------------------------------------------------
# misc. logging functions
def logmsg(level, msg):
    syslog.syslog(level, 'prom-push: %s' % msg)

def logdbg(msg):
    logmsg(syslog.LOG_DEBUG, msg)

def loginfo(msg):
    logmsg(syslog.LOG_INFO, msg)

def logerr(msg):
    logmsg(syslog.LOG_ERR, msg)
