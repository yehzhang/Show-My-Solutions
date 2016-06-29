import os
from collections.abc import Mapping
import pytz
from sqlalchemy import (Column, Table, MetaData, create_engine, ForeignKey, UniqueConstraint,
                        Integer, String, func, DateTime,
                        select, exists, union,
                        event)
from sqlalchemy.engine import Engine


class Submission(Mapping, dict):

    __slots__ = [
        'oj',
        'problem_id',
        'problem_title',
        'problem_url',
        'submit_time',
        'timezone',
        'pid',
    ]

    def __init__(self,
                 oj,
                 problem_id,
                 problem_title,
                 problem_url,
                 submit_time,
                 timezone=None,
                 pid=None):
        """
        :param oj: One of the consistent references to online judges.
        :param problem_title: Should be well formatted because it will be directly saved
            in the database.
        :param submit_time:
            Must be aware if timezone is not specified.
        :param timezone:
            Timezone of submit_time if it is naive. Assume submit_time is of utc timezone.
        :type oj: str
        :type problem_id: str
        :type title: str
        :type url: str
        :type submit_time: datetime
        :type timezone: str
        :type pid: int
        """
        self.oj = oj.lower()
        self.problem_id = problem_id
        self.problem_title = problem_title
        self.problem_url = problem_url
        if submit_time.tzinfo:
            assert timezone is None, 'Both timezone and aware submit_time are given'
            self.submit_time = submit_time
            self.timezone = submit_time.tzinfo.zone
        else:
            assert timezone is not None, 'submit_time is naive'
            self.submit_time = pytz.utc.localize(submit_time).astimezone(pytz.timezone(timezone))
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
        values = self.values()[:-2]
        values[4] = values[4].replace()
        return Submission(*values)

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
          Column('submit_time', DateTime(False), nullable=False),
          Column('timezone', String, nullable=False),
          UniqueConstraint('oj', 'problem_id', name='uix_1'))
t_milestone = Table('milestone', meta,
                    Column('pid', Integer, primary_key=True),
                    Column('handler_name', String, nullable=False),
                    Column('submission_pid', Integer, ForeignKey(t.c.pid), nullable=False),
                    Column('last_modified', DateTime, default=func.now(), nullable=False))
t_login = Table('login', meta,
                Column('pid', Integer, primary_key=True),
                Column('website_name', String, nullable=False),
                Column('user_token', String, nullable=False),
                Column('last_modified', DateTime, default=func.now(), nullable=False))


def start_database(**kwargs):
    global engine
    if engine:
        raise RuntimeError('Database is already started')

    name = kwargs.pop('name', 'sms.db')
    path = kwargs.pop('path', '.')
    url = 'sqlite:///' + os.path.join(path, name)
    engine = create_engine(url, **kwargs)
    meta.create_all(engine)
    return url


def _reset_tables():
    meta.drop_all(engine)
    meta.create_all(engine)


def record_submissions(subs):
    """
    :type subs: [Submission]
    :caller: Scraper

    If multiple 'problem_id's exist, only the one with earliest 'submit_time'
        will be recorded.
    """
    # Remove tzinfo. Some databases do not support timezone info in datetime
    new_subs = []
    for sub in subs:
        sub = sub.clone()
        sub.submit_time = sub.submit_time.astimezone(pytz.utc).replace(tzinfo=None)
        new_subs.append(sub)

    # Let records be a bit more ordered
    new_subs.sort(key=lambda x: x.submit_time)

    ins = t.insert().prefix_with('OR IGNORE').values(new_subs)
    with engine.connect() as conn:
        conn.execute(ins)


def get_lastest_problem_id(oj):
    """
    :param oj:
    :type oj: str, case-insensitive
    :return: Latest recorded 'problem_id' of the uploaded submissions of the given 'oj'.
        None is returned if there is not any 'problem_id' under 'oj'.
    :rtype: str
    :caller: Scraper
    """
    s = select([t.c.problem_id]).where(t.c.oj == oj.lower()).order_by(t.c.pid.desc()).limit(1)
    with engine.connect() as conn:
        for (problem_id,) in conn.execute(s):
            return problem_id


def add_milestone(hdlr_name, milestone):
    """
    :param hdlr_name: Name of the handler that adds a milestone.
    :type hdlr_name: str
    :param milestone: Latest 'pid' of the uploaded submissions.
    :type milestone: int
    :caller: Handler

    Remember to call this function after successful uploading.
    """
    ins = t_milestone.insert().values(handler_name=hdlr_name, submission_pid=milestone)
    with engine.connect() as conn:
        conn.execute(ins)


def fetch_submissions(hdlr_name=None):
    """
    :param hdlr_name: Name of the handler that requests subsmissions.
    :return:
        Return submissions with 'pid' greater than the milestones added by the
        handler with 'handler_name' and sorted by 'pid', which is equivalent to
        sorted by 'submit_time'.
        If hdlr_name is not specified or there are not any milestones under
        the name of a handler, all submissions are returned.
        An empty list is returned if there are no available submissions.
    :rtype: [Submission]
    :caller: Handler
    """
    mlst = select([t_milestone.c.submission_pid, t_milestone.c.handler_name]) \
        .where(t_milestone.c.handler_name == hdlr_name) \
        .order_by(t_milestone.c.submission_pid.desc()).limit(1)
    mlst = union(select([mlst]), select([None, None]).where(~exists(mlst)))
    s = select([t.c.oj,
                t.c.problem_id,
                t.c.problem_title,
                t.c.problem_url,
                t.c.submit_time,
                t.c.timezone,
                t.c.pid]) \
        .where((mlst.c.submission_pid == None) |
               (t.c.pid > mlst.c.submission_pid) & (mlst.c.handler_name == hdlr_name)) \
        .order_by(t.c.pid)
    with engine.connect() as conn:
        return [Submission(*d) for d in conn.execute(s)]


def fetch_user_token(website):
    """
    :param website: name of the website to find 'user_token'
    :type website: str
    :return: latest saved 'user_token' of the given 'website_name'
        None is returned if there does not exist such a record
    :rtype: str
    :caller: Auth
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
    :caller: Auth
    """
    ins = t_login.insert().values(website_name=website, user_token=token)
    with engine.connect() as conn:
        conn.execute(ins)
