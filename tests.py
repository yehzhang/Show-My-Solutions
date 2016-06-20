from datetime import datetime, timedelta
import random
import pytest
from sqlalchemy.exc import IntegrityError
from dbmanager import (record_submissions, fetch_submissions, add_milestone,
                       get_lastest_problem_id, _start_database, _reset_tables)


OJS = ['POJ', 'LeetCode', 'Codeforces', 'TopCoder']
MAX_ROW = 100


_start_database('sqlite:///sms_test.db', echo=False)


def gen_sub(oj, problem_id, seconddelta=None):
    return {
        'oj': oj,
        'problem_id': problem_id,
        'problem_title': 'Test {}'.format(random.randint(0, MAX_ROW)),
        'problem_url': '{}.com/test_{}.html'.format(oj.lower(), problem_id),
        'submit_time': datetime.now() + timedelta(seconds=random.randint(-1e6, 1e6)
                                                  if seconddelta is None else seconddelta)
    }


def refill_submissions(data=None):
    _reset_tables()
    if data is None:
        data = [gen_sub(random.choice(OJS), str(i)) for i in range(MAX_ROW)]
    record_submissions(data)
    return data


def test_record():
    def compare(data):
        result = fetch_submissions()
        data = {(d['oj'], d['problem_id']): dict(d) for d in data}
        data = sorted(data.values(), key=lambda x: x['submit_time'])
        for i, d in enumerate(data):
            del d['problem_id']
            d['pid'] = i + 1
        assert data == result

    data = refill_submissions()
    compare(data)

    # Test duplicated (oj, problem_id)
    new_sub = gen_sub(OJS[0], '-1')
    data.extend([new_sub, new_sub])
    refill_submissions(data)
    compare(data)

    # Record empty list
    _reset_tables()
    record_submissions([])
    compare([])


def test_milestone():
    data = [gen_sub(OJS[0], str(i), i) for i in range(MAX_ROW)]
    refill_submissions(data)

    ms = MAX_ROW / 2
    add_milestone(ms)
    result = fetch_submissions()
    pids = [x['pid'] for x in result]
    assert min(pids) == ms + 1
    assert len(set(pids)) == MAX_ROW - ms
    assert max(pids) == MAX_ROW

    # Out-of-bound milestone should not take effect because of forigen key
    with pytest.raises(IntegrityError):
        add_milestone(MAX_ROW + 1)

    add_milestone(MAX_ROW)
    assert fetch_submissions() == []


def test_latest_problem_id():
    _reset_tables()

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
    for oj in OJS:
        assert get_lastest_problem_id(oj) == latest.get(oj, None)
