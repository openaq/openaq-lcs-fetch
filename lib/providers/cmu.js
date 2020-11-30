/*

  The data are stored in a Google Drive folder at the following address:
  https://drive.google.com/drive/folders/1Mp_a-OyGGlk5tGkezYK41iZ2qybnrPzp?usp=sharing

  This folder contains subfolders categorized by month and year. Each subfolder
  contains CSV files stamped with the date and time.

  Example: ​Location_Data 2020-07-17 11_45.csv

  Please note that the time stamp corresponds to the 15-minute average collected
  15 minutes earlier. For example, the time stamp 11_45 corresponds to the average
  of data collected from 11:15 to 11:30.

  The csv file consists of 13 columns:
    A. InternalSiteName
    B. AnonymizedSiteName
    C. Latitude [deg]
    D. Longitude [deg]
    E. CO [ppb]
    F. NO [ppb]
    G. NO2 [ppb]
    H. O3[ppb]
    I. Pressure [hPa]
    J. PM​2.5 [μg/m​^3 ]
    K. RelativeHumidity [%]
    L. SO​2 [ppb]
    M. Temperature [°C]

  To protect the privacy of our hosts we ask that you do not publish column A
  (Internal Site Name) and use column B (Anonymized Site Name) instead. For the
  same reason, please round columns C and D (Lat and Lon) to the nearest 0.005
  degrees (~250 m radius).

  Some of the fields will be populated with ‘NaN’ values. This is to indicate
  that the site is:
    * Currently unmonitored
    * Not reporting data
    * Has suffered a failure of one or more sensors
*/
/*
  Setup notes:
    - Need OAuth Service Account:
        - https://console.cloud.google.com/iam-admin/serviceaccounts
        - https://developers.google.com/identity/protocols/oauth2/service-account
*/

'use strict';
const { google } = require('googleapis');
const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');

const Providers = require('../providers');

dayjs.extend(utc);


function paginator(drive) {
    return async query => {
        let nextPageToken;
        const files = [];
        let page = 1;
        do {
            console.debug(`Fetching page #${page} where ${JSON.stringify(query)}`);
            const { data } = await drive.files.list({
                ...query,
                pageToken: nextPageToken
            });
            nextPageToken = data.nextPageToken;
            page++;
            files.push(...data.files);
        } while (nextPageToken);
        return files;
    }
}

async function processor(source_name, source) {
    const drive = google.drive({
        version: 'v3',
        auth: new google.auth.GoogleAuth({
            // TODO: Get Oauth from SSM or AWS Secrets Manager, pass in as `credentials`
            keyFile: './alukach-openaq-test-cbba33adc7e3.json',
            scopes: ['https://www.googleapis.com/auth/drive']
        })
    });

    const query = paginator(drive);
    const since = dayjs.utc().subtract(100, 'days');
    const now = dayjs.utc();

    const monthDirs = await query({
        q: `'${source.meta.folderId}' in parents AND (${getMonthQuery(since, now)})`
    })

    console.log({
        q: getFilesQuery(monthDirs.map(({ id }) => id))
    })

    // NOTE: One query with multiple parents does not appear to be working...
    // e.g. '1EbfAX979Jr5Smy_HtPRysPv3-BGZLY3d' in parents OR '1WVWb07YwNqcRgy9fSPKxyr7AOGfnN_GC' in parents
    const files = await Promise.all(
        monthDirs.map(
            ({ id }) => query({ q: `'${id}' in parents` })
        )
    );
    const relevantFiles = files.flat().filter(
        ({ name }) => name > `Location_Data ${since.format('YYYY-MM-DD HH_MM')}.csv`
    );

    console.log({
        before: files.flat().length,
        after: relevantFiles.length
    })

    // const files = await query({
    //     q: getFilesQuery(monthDirs.map(({ id }) => id))
    //     // q: "'1EbfAX979Jr5Smy_HtPRysPv3-BGZLY3d' in parents OR '1WVWb07YwNqcRgy9fSPKxyr7AOGfnN_GC' in parents"
    // })
}

/**
 * Return an array of years and months (represented as a string, formatted as
    such: 'YYYY-MM') that occurred between two times
 * @param {Dayjs} from
 * @param {Dayjs} to
 * @returns {string}
 */
function getMonthQuery(from, to) {
    return [...Array(1 + to.diff(from, 'months')).keys()]
        .map(months => from.add(months, 'months').format('YYYY-MM'))
        .map(yearMonth => `name = '${yearMonth}'`)
        .join(' OR ');
}

function getFilesQuery(parentIds) {
    return parentIds.slice(0, 2)
        .map(id => `'${id}' in parents`)
        .join(' OR ');
}


module.exports = {
    processor
};
