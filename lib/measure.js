const createCsvStringifier = require('csv-writer').createObjectCsvStringifier;

/**
 * @class Measures
 */
class Measures {
    constructor(type) {
        this.headers = [];
        this.measures = [];

        if (type === FixedMeasure) {
            this.headers = ['sensor_id', 'measure', 'timestamp'];
        } else if (type === MobileMeasure) {
            this.headers = ['sensor_id', 'measure', 'timestamp', 'longitude', 'latitude'];
        }
    }

    push(measure) {
        this.measures.push(measure);
    }

    csv() {
        const csvStringifier = createCsvStringifier({
            header: this.headers.map((head) => {
                return {
                    id: head,
                    name: head
                };
            })
        });

        return csvStringifier.getHeaderString() + '\n' + csvStringifier.stringifyRecords(this.measures)
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
            this.measure = params.measure
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
}
