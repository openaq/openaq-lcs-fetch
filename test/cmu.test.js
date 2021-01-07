const tape = require('tape');
const dayjs = require('dayjs')
    .extend(require('dayjs/plugin/utc'))
    .extend(require('dayjs/plugin/timezone'));

const cmu = require('../lib/providers/cmu');

tape('getMonthQuery works for same month', (t) => {
    t.equal(
        cmu.getMonthQuery(
            dayjs('2020-12-01T01:00:00Z').tz('UTC'),
            dayjs('2020-12-02T01:00:01Z').tz('UTC')
        ),
        "name = '2020-12'"
    );
    t.end();
});

tape('getMonthQuery works for same month', (t) => {
    t.equal(
        cmu.getMonthQuery(
            dayjs('2020-12-01T01:00:00Z').tz('UTC'),
            dayjs('2021-01-02T01:00:01Z').tz('UTC')
        ),
        "name = '2020-12' OR name = '2021-01'"
    );
    t.end();
});

tape('getMonthQuery works for multiple years', (t) => {
    t.equal(
        cmu.getMonthQuery(
            dayjs('2019-12-01T01:00:00Z').tz('UTC'),
            dayjs('2021-01-02T01:00:01Z').tz('UTC')
        ),
        "name = '2019-12' OR name = '2020-01' OR name = '2020-02' OR name = '2020-03' OR name = '2020-04' OR name = '2020-05' OR name = '2020-06' OR name = '2020-07' OR name = '2020-08' OR name = '2020-09' OR name = '2020-10' OR name = '2020-11' OR name = '2020-12' OR name = '2021-01'"
    );
    t.end();
});
