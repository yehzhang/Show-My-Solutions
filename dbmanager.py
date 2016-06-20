from sqlalchemy import (Column, Table, MetaData, create_engine, ForeignKey, UniqueConstraint,
                        Integer, String, func, DateTime,
                        select, exists,
                        event)
from sqlalchemy.engine import Engine


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


engine = None
meta = MetaData()
t = Table('submissions', meta,
          Column('pid', Integer, primary_key=True),
          Column('oj', String, nullable=False),
          Column('problem_id', String, nullable=False),
          Column('problem_title', String, nullable=False),
          Column('problem_url', String, nullable=False),
          Column('submit_time', DateTime(True), nullable=False),
          UniqueConstraint('oj', 'problem_id', name='uix_1'))
t_milestone = Table('milestone', meta,
                    Column('pid', Integer, primary_key=True),
                    Column('milestone_pid', Integer, ForeignKey(t.c.pid)),
                    Column('last_modified', DateTime, default=func.now()))


def _start_database(*args, **kwargs):
    global engine
    engine = create_engine(*args, **kwargs)
    meta.create_all(engine)
    return engine

_start_database('sqlite:///sms.db')


def _reset_tables():
    meta.drop_all(engine)
    meta.create_all(engine)


def record_submissions(subs):
    """
    :param subs:
        'oj' should be one of the consistent references to online judges.
        If multiple 'problem_id's exist, only the one with earliest 'submit_time' will be
        recorded.
        'problem_title' should be well formatted because it will be directly saved in the
        database.
        'submit_time' is of type datetime because there is a handy method datetime.strptime.
        Remember to set the timezone of submit time (probably by the location of the host).
    :type subs: [
        {
            'oj': str,
            'problem_id': str,
            'problem_title': str,
            'problem_url': str,
            'submit_time': datetime,
        }
    ]
    """
    subs.sort(key=lambda x: x['submit_time'])
    ins = t.insert().prefix_with('OR IGNORE').values(subs)
    with engine.connect() as conn:
        conn.execute(ins)


def get_lastest_problem_id(oj):
    """
    :param oj:
    :type oj: str
    :return: latest recorded 'problem_id' of the uploaded submissions of the given 'oj'
        None is returned if there is not any 'problem_id' under 'oj'
    :rtype: str
    """
    s = select([t.c.problem_id]).where(t.c.oj == oj).order_by(t.c.pid.desc()).limit(1)
    with engine.connect() as conn:
        for (problem_id,) in conn.execute(s):
            return problem_id


def add_milestone(milestone):
    """
    :param milestone: Latest 'pid' of the uploaded submissions.
    :type milestone: int

    Remember to call this function after successful uploading.
    """
    ins = t_milestone.insert().values(milestone_pid=milestone)
    with engine.connect() as conn:
        conn.execute(ins)


def fetch_submissions():
    """
    :return:
        Submissions with 'pid' greater than the milestone and sorted by 'submit_time'.
        An empty list is returned if there are no available submissions.
    :rtype: [
        {
            'pid': int,
            'oj': str,
            'problem_title': str,
            'problem_url': str,
            'submit_time': datetime,
        }
    ]
    """
    c_milestone = t_milestone.c.milestone_pid
    milestone = select([c_milestone]).order_by(c_milestone.desc()).limit(1)
    s = select([t.c.pid, t.c.oj, t.c.problem_title, t.c.problem_url, t.c.submit_time]) \
        .where(~exists(milestone) | (t.c.pid > milestone)) \
        .order_by(t.c.submit_time)
    with engine.connect() as conn:
        return [
            {
                'pid': pid,
                'oj': oj,
                'problem_title': title,
                'problem_url': url,
                'submit_time': time
            }
            for (pid, oj, title, url, time) in conn.execute(s)
        ]
