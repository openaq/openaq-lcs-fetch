/**
 * NOTES:
 *   - The Google Sheet that is associated with this provider must be explicitely shared
 *     with the Google OAuth Service Account. https://stackoverflow.com/a/49965912/728583
 */

const { google } = require("googleapis");
const { fetchSecret, VERBOSE, toCamelCase } = require("../lib/utils");

async function processor(source_name, source) {
  const credentials = await fetchSecret(source_name);
  const orgs = await getOrganizations(source.meta.spreadsheetId, credentials);
  return Promise.all(orgs.map(syncOrganizationData));
}

async function syncOrganizationData(org) {
  console.log({ org });
}

/**
 * Fetch Clarity organizations from management worksheet
 * @param {string} spreadsheetId
 * @param {string} credentials
 * @returns
 */
async function getOrganizations(spreadsheetId, credentials) {
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

module.exports = {
  processor,
};
