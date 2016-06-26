import logging
from datetime import datetime, timedelta
import random
import pytest
import pytz
from show_my_solutions.dbmanager import Submission


OJS = ['POJ', 'LeetCode', 'Codeforces', 'TopCoder', 'HackerRank', 'ACM']
MAX_ROW = 100

LOGGER = logging.getLogger(__name__)


def setup_module(module):
    from show_my_solutions.dbmanager import start_database
    start_database(name='sms_test.db', echo=False)


def gen_sub(oj, problem_id, seconddelta=None):
    seconddelta = random.randint(-1e6, 1e6) if seconddelta is None else seconddelta
    return Submission(oj,
                      problem_id,
                      'Test {}'.format(random.randint(0, MAX_ROW)),
                      '{}.com/test_{}.html'.format(oj.lower(), problem_id),
                      datetime.now(tz=pytz.utc) + timedelta(seconds=seconddelta))


def gen_rand_subs(n=MAX_ROW, sort=False):
    data = [gen_sub(random.choice(OJS), str(i)) for i in range(n)]
    if sort:
        data.sort(key=lambda x: x.submit_time)
    return data


def refill_submissions(data=None):
    from show_my_solutions.dbmanager import record_submissions, _reset_tables

    _reset_tables()
    if data is None:
        data = gen_rand_subs()
    record_submissions(data)
    return data


def test_record():
    from show_my_solutions.dbmanager import (fetch_submissions, record_submissions)

    def compare(data):
        result = fetch_submissions()
        data = {(d.oj, d.problem_id): d.clone() for d in data}
        data = sorted(data.values(), key=lambda x: x.submit_time)
        for i, d in enumerate(data):
            d.pid = i + 1
            d.oj = d.oj.lower()
        assert data == result

    data = refill_submissions()
    compare(data)

    # Test duplicated (oj, problem_id)
    new_sub = gen_sub(OJS[0], '-1')
    data.extend([new_sub, new_sub])
    refill_submissions(data)
    compare(data)

    # Record empty list
    refill_submissions([])
    compare([])


def test_milestone():
    from sqlalchemy.exc import IntegrityError
    from show_my_solutions.dbmanager import fetch_submissions, add_milestone

    data = [gen_sub(OJS[0], str(i), i) for i in range(MAX_ROW)]
    refill_submissions(data)

    ms = MAX_ROW / 2
    add_milestone('rand_tester', ms)
    result = fetch_submissions('rand_tester')
    pids = [x.pid for x in result]
    assert min(pids) == ms + 1
    assert len(set(pids)) == MAX_ROW - ms
    assert max(pids) == MAX_ROW

    # Out-of-bound milestone should not take effect because of forigen key
    with pytest.raises(IntegrityError):
        add_milestone('rand_tester', MAX_ROW + 1)

    add_milestone('rand_tester', MAX_ROW)
    assert fetch_submissions('rand_tester') == []


def test_latest_problem_id():
    from show_my_solutions.dbmanager import (record_submissions, get_lastest_problem_id)

    refill_submissions([])

    for oj in OJS:
        assert get_lastest_problem_id(oj) is None

    latest = {}
    data = []
    for i in range(MAX_ROW):
        oj = random.choice(OJS)
        problem_id = str(random.randint(0, 1e5))
        latest[oj] = problem_id
        data.append(gen_sub(oj, problem_id, i))
    record_submissions(data)
    LOGGER.debug('\n'.format(map(str, data)))
    for oj in OJS:
        assert get_lastest_problem_id(oj) == latest.get(oj, None)


@pytest.fixture
def reactor():
    from show_my_solutions.app import get_config, Reactor
    return Reactor(get_config())


def test_trello_handler(reactor):
    from show_my_solutions.handlers import build_handler

    handler = build_handler('trello', reactor)
    handler.upload(gen_rand_subs(10, True))


def test_leetcode_scraper(reactor):
    from show_my_solutions.scrapers import build_scraper
    from bs4 import BeautifulSoup
    # _reset_tables()

    lcs = build_scraper('leetcode', reactor)
    lcs.fetch()
