import datetime
import logging
import time
import os
import json
import paho.mqtt.client as mqtt

from collections import defaultdict, OrderedDict

import prometheus_client
import prometheus_client.core
import psycopg2
from psycopg2 import errorcodes as postgres_errors

from ctf_gameserver.lib import daemon
from ctf_gameserver.lib.args import get_arg_parser_with_db, parse_host_port
from ctf_gameserver.lib.database import transaction_cursor
from ctf_gameserver.lib.exceptions import DBDataError
from ctf_gameserver.lib.metrics import start_metrics_server

from . import database

MOSQUITTO_BROKER_IP = "10.255.253.220"
MOSQUITTO_BORKER_PORT = 1883

def main():

    arg_parser = get_arg_parser_with_db('CTF MQTT Publisher')
    arg_parser.add_argument('--nonstop', action='store_true', help='Use current time as start time and '
                            'ignore CTF end time from the database. Useful for testing checkers.')
    arg_parser.add_argument('--metrics-listen', help='Expose Prometheus metrics via HTTP ("<host>:<port>")')

    args = arg_parser.parse_args()

    logging.basicConfig(format='[%(levelname)s] %(message)s')
    numeric_loglevel = getattr(logging, args.loglevel.upper())
    logging.getLogger().setLevel(numeric_loglevel)

    try:
        db_conn = psycopg2.connect(host=args.dbhost, database=args.dbname, user=args.dbuser,
                                   password=args.dbpassword)
    except psycopg2.OperationalError as e:
        logging.error('Could not establish database connection: %s', e)
        return os.EX_UNAVAILABLE
    logging.info('Established database connection')

    # Keep our mental model easy by always using (timezone-aware) UTC for dates and times
    with transaction_cursor(db_conn) as cursor:
        cursor.execute('SET TIME ZONE "UTC"')

    # Check database grants
    try:
        try:
            database.get_control_info(db_conn, prohibit_changes=True)
        except DBDataError as e:
            logging.warning('Invalid database state: %s', e)

    except psycopg2.ProgrammingError as e:
        if e.pgcode == postgres_errors.INSUFFICIENT_PRIVILEGE:
            # Log full exception because only the backtrace will tell which kind of permission is missing
            logging.exception('Missing database permissions:')
            return os.EX_NOPERM
        else:
            raise

    daemon.notify('READY=1')

    now = datetime.datetime.now(datetime.timezone.utc)

    game_info = {
        "type": None,
        "timestamp": now,
        "active": False,
        "ssid": None,
        "FinishTime": None,
        "attacker": 0,
        "victim": 0,
        "service": 0,
        "teamlist": [],
        "servicelist": []
    }

    last_capture = -1
    current_tick = 0

    while True:
        game_info, last_capture, current_tick = main_loop_step(db_conn, game_info, last_capture, current_tick)


def main_loop_step (db_conn, game_info, last_capture, current_tick): 

    def sleep(duration):
        logging.info('Sleeping for %d seconds', duration)
        time.sleep(duration)

    #Get control info from database
    try:
        control_info = database.get_control_info(db_conn)
    except DBDataError as e:
        logging.warning('Invalid database state: %s', e)
        sleep(30)
        return game_info, last_capture, current_tick

   # These fields are allowed to be NULL
    if control_info['start'] is None or control_info['end'] is None:
        logging.warning('Competition start and end time must be configured in the database')
        sleep(30)
        return game_info, last_capture, current_tick

    now = datetime.datetime.now(datetime.timezone.utc)

    #Start
    if game_info["active"] == False:
        if now >= control_info['start'] and now < control_info['end']:
            game_info["type"] = "S"
            game_info["timestamp"] = int(now.timestamp())
            game_info["active"] = True

            saio_id, talde_info, service_info = database.get_general_info(db_conn)

            game_info["ssid"] = saio_id
            game_info["FinishTime"] = int(control_info['end'].timestamp())
            talde_kopurua = len(talde_info)

            for i in range(talde_kopurua):
                game_info["teamlist"].append({})
                game_info["teamlist"][i]["tid"] = talde_info[i][0]
                game_info["teamlist"][i]["tname"] = talde_info[i][1]
                game_info["teamlist"][i]["atkcount"] = 0
                game_info["teamlist"][i]["viccount"] = 0
                game_info["teamlist"][i]["service"] = 0
                game_info["teamlist"][i]["SLA"] = 100
                game_info["teamlist"][i]["score"] = 0

            for i in range(len(service_info)):
                game_info["servicelist"].append({})
                game_info["servicelist"][i]["sid"] = service_info[i][0]
                game_info["servicelist"][i]["sname"] = service_info[i][1]

            current_tick = 1

            _publish_status(game_info)


    if game_info["active"] == True:

        #Finish
        if now >= control_info['end']+datetime.timedelta(seconds=2):
            game_info["type"] = "F"
            game_info["timestamp"] = int(now.timestamp())
            game_info["active"] = False
            game_info["attacker"] = 0
            game_info["victim"] = 0
            game_info["service"] = 0
        
            game_info = _update_slas(game_info, db_conn, current_tick) 
            game_info = _update_scores(game_info, db_conn)

            current_tick = control_info["current_tick"]

            _publish_status(game_info)
            return game_info, last_capture, current_tick

        #Publish if there are new Ticks
        if control_info["current_tick"] > current_tick:
            current_tick = control_info["current_tick"]
            game_info["type"] = "T"
            game_info["timestamp"] = int(now.timestamp())
            game_info["attacker"] = 0
            game_info["victim"] = 0
            game_info["service"] = 0

            sleep(2)
            game_info = _update_slas(game_info, db_conn, current_tick)
            game_info = _update_scores(game_info, db_conn)  

            _publish_status(game_info)

        #Get new captures and publish them
        new_captures = database.get_new_captures(db_conn, last_capture)

        for capture in new_captures:
            capture_id, capturing_team_id, protecting_team_id, service_id = capture
            capturing_team_id_location = next((index for index, team in enumerate(game_info["teamlist"]) if team["tid"] == capturing_team_id), None)
            protecting_team_id_location = next((index for index, team in enumerate(game_info["teamlist"]) if team["tid"] == protecting_team_id), None)
            game_info["type"] = "C"
            game_info["timestamp"] = int(now.timestamp())
            game_info["attacker"] = capturing_team_id
            game_info["victim"] = protecting_team_id
            game_info["service"] = service_id
            game_info["teamlist"][capturing_team_id_location]["atkcount"] += 1
            game_info["teamlist"][protecting_team_id_location]["viccount"] += 1

            game_info = _update_scores(game_info, db_conn) 

            last_capture = capture_id
            sleep(1)
            _publish_status(game_info)

            logging.info('Status published')

    return game_info, last_capture, current_tick


def get_sleep_seconds(control_info, now=None):    #get_sleep_seconds(control_info, metrics, now=None):
    """
    Returns the number of seconds until the next tick starts.
    """

    if now is None:
        now = datetime.datetime.now(datetime.timezone.utc)

    next_tick_start_offset = (control_info['current_tick'] + 1) * control_info['tick_duration']
    next_tick_start = control_info['start'] + datetime.timedelta(seconds=next_tick_start_offset)
    until_next_tick = next_tick_start - now
    until_next_tick_secs = until_next_tick.total_seconds()

    return max(until_next_tick_secs, 0)

def _update_slas(game_info, db_conn, tick):
    scores = database.get_sla_scores(db_conn)

    max_score = (tick-1) * 2.5 * 6

    for i, score in enumerate(scores):
        game_info["teamlist"][i]["SLA"] = scores 

    return game_info

def _update_scores(game_info, db_conn):
    scores = database.get_scores(db_conn)

    for i, score in enumerate(scores):
        game_info["teamlist"][i]["score"] = round(score, 0) 

    return game_info

def _publish_status(value):

    tmp_value = value.copy()
    tmp_value["timestamp"] = tmp_value["timestamp"] + 7200 
    tmp_value["FinishTime"] = tmp_value["FinishTime"] + 7200

    client = mqtt.Client()
    client.connect(MOSQUITTO_BROKER_IP, MOSQUITTO_BORKER_PORT)
    client.publish("status", json.dumps(tmp_value))
    client.disconnect()

if __name__ == '__main__':
    raise SystemExit(main())