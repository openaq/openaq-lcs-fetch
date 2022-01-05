const createCsvStringifier = require('csv-writer').createObjectCsvStringifier;
const {
  putObject
} = require('./utils');


/**
 * @class Measures
 */
class Measures {
  constructor(type, file) {
    this.headers = [];
    this.measures = [];
    this.file = { name: 'uk-file' };

    if (type === FixedMeasure) {
      this.headers = ['sensor_id', 'measure', 'timestamp'];
    } else if (type === MobileMeasure) {
      this.headers = ['sensor_id', 'measure', 'timestamp', 'longitude', 'latitude'];
    }
    if(file) {
      this.file = file;
    }
  }

    push(measure) {
        this.measures.push(measure);
    }

    get length() {
        return this.measures.length;
    }

    csv() {
        const csvStringifier = createCsvStringifier({
            header: this.headers.map((head) => ({
                id: head,
                title: head
            }))
        });

        return csvStringifier.stringifyRecords(this.measures);
    }

  key() {
    const stack = process.env.STACK;
    const provider = process.env.PROVIDER;
    const name = this.file.name.endsWith('.csv')
          ? this.file.name.slice(0, -4)
          : this.file.name;

    return `${stack}/measures/${provider}/${name}.csv.gz`;
  }

  async put() {
    const key = this.key();
    //console.debug('PUTTING MEASURES', key);
    await putObject(this, key);
    return true;
  }

}

/**
 * @class Measure
 */
class Measure {
    constructor(params = {}) {
        this.sensor_id = params.sensor_id || null;
        this.timestamp = params.timestamp || null;

        if (params.measure !== null && params.measure !== undefined) {
            this.measure = params.measure;
        }
    }
}

/**
 * @class FixedMeasure
 */
class FixedMeasure extends Measure {
    constructor(params) {
        super(params);
    }
}

/**
 * @class MobileMeasure
 */
class MobileMeasure extends Measure {
    constructor(params) {
        super(params);

        this.latitude = params.latitude || null;
        this.longitude = params.longitude || null;
    }
}

module.exports = {
    Measures,
    FixedMeasure,
    MobileMeasure
};
