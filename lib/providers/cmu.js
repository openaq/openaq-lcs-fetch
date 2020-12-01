/**

  The data are stored in a Google Drive folder at the following address:
  https://drive.google.com/drive/folders/1Mp_a-OyGGlk5tGkezYK41iZ2qybnrPzp?usp=sharing

  This folder contains subfolders categorized by month and year. Each subfolder
  contains CSV files stamped with the date and time.

  Example: Location_Data 2020-07-17 11_45.csv

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
    J. PM2.5 [μg/m^3 ]
    K. RelativeHumidity [%]
    L. SO2 [ppb]
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
const { google, drive_v3 } = require('googleapis');
const csvParser = require('csv-parser');
const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc'); // dependent on utc plugin
const timezone = require('dayjs/plugin/timezone');
dayjs.extend(utc);
dayjs.extend(timezone);

// const Providers = require('../providers');

async function processor(source_name, source) {
    // eslint-disable-next-line no-use-before-define
    const drive = new DriveFiles({
        auth: new google.auth.GoogleAuth({
            // TODO: Get Oauth from SSM or AWS Secrets Manager, pass in as `credentials`
            keyFile: './alukach-openaq-test-cbba33adc7e3.json',
            scopes: ['https://www.googleapis.com/auth/drive']
        })
    });

    // NOTE: Times are recorded in EST
    const since = dayjs().tz('America/New_York').subtract(1, 'hour');
    const now = dayjs().tz('America/New_York');

    // Get directory for each month's readings
    const monthDirs = await drive.query({
        q: `'${source.meta.folderId}' in parents AND (${getMonthQuery(since, now)})`
    });

    console.debug(`Found ${monthDirs.length} month directories to process...`);

    // For whatever reason, we're unable to query Drive for multiple parents.
    // Instead, we'll handle each month directory concurrently.
    await Promise.all(
        monthDirs
            .map(async (monthDir) => {
                const files = await drive.query(
                    { q: `'${monthDir.id}' in parents` },
                    getFilename(since)
                );

                for (const file of files) {
                    for (const row of await drive.parseCsv(file.id)) {
                        console.log(row);
                    }
                }
            })
    );
}


/**
 * Return an array of years and months (represented as a string, formatted as
    such: 'YYYY-MM') that occurred between two times
 *
 * @param {dayjs.Dayjs} from Start time
 * @param {dayjs.Dayjs} to End time
 *
 * @returns {string}
 */
function getMonthQuery(from, to) {
    return [...Array(1 + to.diff(from, 'months')).keys()]
        .map((months) => from.add(months, 'months').format('YYYY-MM'))
        .map((yearMonth) => `name = '${yearMonth}'`)
        .join(' OR ');
}


/**
 * Return the expected filename of a CSV written at the provided timestamp.
 *
 * @param {dayjs.Dayjs} timestamp
 *
 * @returns {string}
 */
function getFilename(timestamp) {
    return `Location_Data ${timestamp.format('YYYY-MM-DD HH_MM')}.csv`;
}

/**
 * Convenience wrapper around Google Drive's Files API.
 */
class DriveFiles extends drive_v3.Resource$Files {

    constructor(options, google) {
        super({
            _options: options || {},
            google
        });
    }

    // eslint-disable-next-line valid-jsdoc
    /**
     * A pagination-friendly wrapper around the list() method.
     *
     * @param {import("googleapis").drive_v3.Params$Resource$Files$List} params
     * @param {String} [greater_than]
     *
     * @returns {Promise<import("googleapis").drive_v3.Schema$FileList>}
     */
    async query(params, greater_than) {
        let nextPageToken;
        const files = [];
        let page = 1;

        do {
            console.debug(`Fetching page #${page} of ${JSON.stringify(params)}`);

            const { data } = await this.list({
                orderBy: 'name desc',
                ...params,
                pageToken: nextPageToken
            });

            for (const file of data.files) {
                if (greater_than && file.name < greater_than) {
                    console.debug(`File ${file.name} is less than ${greater_than}, stopping requests...`);
                    return files;
                }

                files.push(file);
            }

            page++;
            nextPageToken = data.nextPageToken;
        } while (nextPageToken);

        return files;
    }

    /**
     * Retrieve rows from a CSV stored within Google Drive.
     *
     * @param {string} fileId
     *
     * @returns {Promise<object[]>}
     */
    async parseCsv(fileId) {
        const { data } = await this.get({
            fileId,
            alt: 'media'
        }, { responseType: 'stream' });

        return new Promise((resolve, reject) => {
            const rows = [];
            data
                .pipe(csvParser())
                .on('error', reject)
                .on('data', (row) => rows.push(row))
                .on('end', () => resolve(rows));
        });
    }
}

module.exports = {
    processor
};
