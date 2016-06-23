import os
from collections.abc import Mapping
import pytz
from sqlalchemy import (Column, Table, MetaData, create_engine, ForeignKey, UniqueConstraint,
                        Integer, String, func, DateTime,
                        select, exists,
                        event)
from sqlalchemy.engine import Engine


class Submission(Mapping, dict):

    __slots__ = ['oj', 'problem_id', 'problem_title', 'problem_url', 'submit_time', 'pid', 'timezone']

    def __init__(self, oj, problem_id, problem_title, problem_url, submit_time, pid=None, timezone=None):
        """
        :param oj: One of the consistent references to online judges.
        :param problem_title: Should be well formatted because it will be directly saved
            in the database.
        :param submit_time:
            Must be aware.
            There is a handy method called datetime.strptime.
            Remember to set its timezone (probably by the location of the host).
        :type oj: str
        :type problem_id: str
        :type title: str
        :type url: str
        :type time: datetime
        """
        self.oj = oj.lower()
        self.problem_id = problem_id
        self.problem_title = problem_title
        self.problem_url = problem_url
        if submit_time.tzinfo:
            assert timezone is None, 'Both aware submit_time and timezone are given'
            self.timezone = submit_time.tzinfo.zone
            self.submit_time = submit_time
        else:
            assert timezone is not None, 'Datetime is naive'
            self.submit_time = pytz.timezone(timezone).localize(submit_time)
            self.timezone = timezone
        self.pid = pid

    def __eq__(self, other):
        return self.values()[:-1] == other.values()[:-1]

    def __getitem__(self, key):
        try:
            return getattr(self, key)
        except AttributeError:
            raise KeyError(key) from None

    def __iter__(self):
        return iter(self.__slots__)

    def __len__(self):
        return len(self.__slots__)

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return 'Submission({})'.format(', '.join(map(str, self.values())))

    def clone(self):
        return Submission(*self.values()[:-1])

    def values(self):
        return [self[k] for k in self]

    def items(self):
        return [(k, self[k]) for k in self]


@event.listens_for(Engine, "connect")
def _set_sqlite_pragma(dbapi_connection, connection_record):
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
          Column('timezone', String, nullable=False),
          UniqueConstraint('oj', 'problem_id', name='uix_1'))
t_milestone = Table('milestone', meta,
                    Column('pid', Integer, primary_key=True),
                    Column('milestone_pid', Integer, ForeignKey(t.c.pid), nullable=False),
                    Column('last_modified', DateTime(True), default=func.now(), nullable=False))
t_login = Table('login', meta,
                Column('pid', Integer, primary_key=True),
                Column('website_name', String, nullable=False),
                Column('user_token', String, nullable=False),
                Column('last_modified', DateTime, default=func.now(), nullable=False))


def start_database(name='sms.db', path='.', *args, **kwargs):
    global engine
    url = 'sqlite:///' + os.path.join(path, name)
    engine = create_engine(url, *args, **kwargs)
    meta.create_all(engine)


def _reset_tables():
    meta.drop_all(engine)
    meta.create_all(engine)


def record_submissions(subs):
    """
    :type subs: [Submission]

    If multiple 'problem_id's exist, only the one with earliest 'submit_time'
        will be recorded.
    """
    subs.sort(key=lambda x: x.submit_time)
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
    :rtype: [Submission]
    """
    c_milestone = t_milestone.c.milestone_pid
    milestone = select([c_milestone]).order_by(c_milestone.desc()).limit(1)
    c = t.c
    s = select([c.oj, c.problem_id, c.problem_title, c.problem_url, c.submit_time, c.pid, c.timezone]) \
        .where(~exists(milestone) | (t.c.pid > milestone)) \
        .order_by(t.c.submit_time)
    with engine.connect() as conn:
        return [Submission(*d) for d in conn.execute(s)]

def fetch_user_token(website):
    """
    :param website: name of the website to find 'user_token'
    :type website: str
    :return: latest saved 'user_token' of the given 'website_name'
        None is returned if there does not exist such a record
    :rtype: str
    """
    s = select([t_login.c.user_token]) \
        .where(t_login.c.website_name == website) \
        .order_by(t_login.c.pid.desc()) \
        .limit(1)
    with engine.connect() as conn:
        for (token,) in conn.execute(s):
            return token

def save_user_token(website, token):
    """
    :param website: name of the website to save 'user_token'
    :type website: str
    :type token: str
    """
    ins = t_login.insert().values(website_name=website, user_token=token)
    with engine.connect() as conn:
        conn.execute(ins)
