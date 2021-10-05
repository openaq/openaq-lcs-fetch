/**
 * NOTES:
 *   - The Google Sheet that is associated with this provider must be explicitely shared
 *     with the Google OAuth Service Account. https://stackoverflow.com/a/49965912/728583
 */

const { google } = require("googleapis");
const dayjs = require("dayjs");
const { fetchSecret, VERBOSE, toCamelCase, request } = require("../lib/utils");

/**
 * Fetch Clarity organizations from management worksheet
 * @param {string} spreadsheetId
 * @param {string} credentials
 * @returns {Organization[]}
 */
async function listOrganizations(spreadsheetId, credentials) {
  if (VERBOSE) console.debug(`Fetching Google sheet ${spreadsheetId}...`);
  const sheets = google.sheets({
    version: "v4",
    auth: new google.auth.GoogleAuth({
      credentials,
      scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"],
    }),
  });
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId,
    // We use a named range for the Org Name & API Key
    // https://support.google.com/docs/answer/63175
    range: "Credentials",
  });

  const rows = res.data.values;
  if (VERBOSE)
    console.debug(`Retrieved ${rows.length} rows from the Google sheet`);
  if (rows.length <= 1) return [];

  const columns = rows.shift().map(toCamelCase);
  return rows.map((row) =>
    Object.assign({}, ...row.map((col, i) => ({ [columns[i]]: col })))
  );
}

class ClarityApi {
  /**
   *
   * @param {String} baseUrl
   * @param {Organization} org
   */
  constructor(baseUrl, org) {
    this.baseUrl = baseUrl;
    this.org = org;
  }

  /**
   *
   * @returns {Device[]}
   */
  listDevices() {
    if (VERBOSE)
      console.log(`Listing devices for ${this.org.organizationName}...`);
    return request({
      json: true,
      method: "GET",
      headers: { "X-API-Key": this.org.apiKey },
      url: new URL("v1/devices", this.baseUrl),
    }).then((response) => response.body);
  }

  /**
   *
   * @param {String} code
   * @param {dayjs.Dayjs} since
   * @param {dayjs.Dayjs} to
   * @yields {Measurement}
   */
  async *fetchMeasurements(code, since, to) {
    if (VERBOSE)
      console.log(
        `Fetching measurements for ${this.org.organizationName}/${code}...`
      );

    const limit = 5;
    let offset = 0;

    const url = new URL("v1/measurements", this.baseUrl);
    url.searchParams.set("code", code);
    url.searchParams.set("limit", limit);
    url.searchParams.set("startTime", since.toISOString());
    url.searchParams.set("endTime", to.toISOString());

    while (true) {
      url.searchParams.set("skip", offset);
      console.log(`Fetching ${url}`);
      const response = await request({
        url,
        json: true,
        method: "GET",
        headers: { "X-API-Key": this.org.apiKey },
        gzip: true,
      });

      for (let measurment of response.body) {
        yield measurment;
      }

      // More data to fetch
      if (response.body.length === limit) {
        offset += limit;
        continue;
      }

      if (VERBOSE)
        console.log(
          `Got ${response.body.length} of ${limit} possible measurements for device ${code} at offset ${offset}, stopping pagination.`
        );
      // All done
      break;
    }
  }

  /**
   *
   * @param {String} organizationName
   */
  async sync() {
    const devices = await this.listDevices();
    const now = dayjs();

    for (const device of devices) {
      const measurements = this.fetchMeasurements(
        device.code,
        now.subtract(1.25, "hour"),
        now
      );
      for await (const measure of measurements) {
        console.log({ measure });
      }
    }
  }
}

/**
 * @typedef {Object} Organization
 *
 * @property {String} apiKey
 * @property {String} organizationName
 */

/**
 * @typedef {Object} Device
 *
 * @property {String} _id
 * @property {String} code
 * @property {('purchased'|'configured'|'working'|'decommisioned')} lifeStage
 * @property {Array} enabledCharacteristics
 * @property {Object} state
 * @property {Object} location
 * @property {Boolean} indoor
 * @property {'2021-07-24T16:47:27.736Z'} workingStartAt
 * @property {'2021-10-01T04:40:47.174Z'} lastReadingReceivedAt
 * @property {('nominal'|'degraded'|'critical')} sensorsHealthStatus
 * @property {('needsSetup'|'needsAttention'|'healthy')} overallStatus
 */

module.exports = {
  async processor(source_name, source) {
    const credentials = await fetchSecret(source_name);
    const orgs = await listOrganizations(
      source.meta.spreadsheetId,
      credentials
    );
    return Promise.all(
      orgs.map((org) => new ClarityApi(source.meta.url, org).sync())
    );
  },
};
