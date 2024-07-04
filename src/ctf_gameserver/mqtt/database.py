from ctf_gameserver.lib.database import transaction_cursor
from ctf_gameserver.lib.date_time import ensure_utc_aware
from ctf_gameserver.lib.exceptions import DBDataError


def get_control_info(db_conn, prohibit_changes=False):
    """
    Returns a dictionary containing relevant information about the competion, as stored in the database.
    """

    with transaction_cursor(db_conn, prohibit_changes) as cursor:
        cursor.execute('SELECT start, "end", tick_duration, current_tick FROM scoring_gamecontrol')
        result = cursor.fetchone()

    if result is None:
        raise DBDataError('Game control information has not been configured')
    start, end, duration, tick = result

    return {
        'start': ensure_utc_aware(start),
        'end': ensure_utc_aware(end),
        'tick_duration': duration,
        'current_tick': tick
    }

def get_general_info(db_conn, prohibit_changes=False):

    with transaction_cursor(db_conn, prohibit_changes) as cursor:

        cursor.execute('SELECT id FROM scoring_gamecontrol')
        session_aktibo_id = cursor.fetchone()[0]

        cursor.execute('SELECT u.id AS tid, username AS tname FROM registration_team t INNER JOIN auth_user u ON t.user_id = u.id WHERE is_active')
        team_info = cursor.fetchall()

        cursor.execute('SELECT id, name FROM scoring_service')
        service_info = cursor.fetchall()

        return session_aktibo_id, team_info, service_info

def get_exploiting_teams_counts(db_conn, prohibit_changes=False):

    with transaction_cursor(db_conn, prohibit_changes) as cursor:
        cursor.execute('SELECT service.slug, COUNT(DISTINCT capture.capturing_team_id)'
                       '    FROM scoring_service service'
                       '    JOIN scoring_flag flag ON flag.service_id = service.id'
                       '    LEFT JOIN (SELECT * FROM scoring_capture) AS capture'
                       '        ON capture.flag_id = flag.id'
                       '    GROUP BY service.slug')
        counts = cursor.fetchall()

    return dict(counts)

def get_new_captures(db_conn, last_capture, prohibit_changes=False):
    #Returns a list of captures that have been made since the last capture.

    with transaction_cursor(db_conn, prohibit_changes) as cursor:
        cursor.execute('SELECT capture.id, capture.capturing_team_id, flag.protecting_team_id, flag.service_id'
                       '    FROM scoring_capture capture INNER JOIN scoring_flag flag ON capture.flag_id = flag.id'
                       '    WHERE capture.id > %s'
                       '    ORDER BY capture.id', (last_capture,))
        captures = cursor.fetchall()

    return captures

def get_scores(db_conn, prohibit_changes=False):
    #Returns a list of scores of all teams.

    with transaction_cursor(db_conn, prohibit_changes) as cursor:
        cursor.execute('SELECT team_id, sum(total) FROM scoring_scoreboard GROUP BY team_id ORDER BY team_id')
        scores = cursor.fetchall()

    return [score[1] for score in scores]    

def get_sla_scores(db_conn, prohibit_changes=False):
    #Returns a list of scores of all teams.

    with transaction_cursor(db_conn, prohibit_changes) as cursor:
        cursor.execute('SELECT ok.team_id, (ok.c * 100 / total.c) AS guztira \
                        FROM (select team_id, count(*) as c from scoring_statuscheck where status = 0 group by team_id order by team_id) AS ok INNER JOIN \
                             (select team_id, count(*) as c from scoring_statuscheck group by team_id order by team_id) AS total on ok.team_id = total.team_id;')
        scores = cursor.fetchall()

    return [score[1] for score in scores]   

