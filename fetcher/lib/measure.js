const createCsvStringifier = require('csv-writer').createObjectCsvStringifier;

/**
 * @class Measures
 */
class Measures {
    constructor(type) {
        this.headers = [];
        this.measures = [];
				this.from = null;
				this.to = null;

        if (type === FixedMeasure) {
            this.headers = ['sensor_id', 'measure', 'timestamp'];
        } else if (type === MobileMeasure) {
            this.headers = ['sensor_id', 'measure', 'timestamp', 'longitude', 'latitude'];
        }
    }

    push(measure) {
				if(!this.to || measure.timestamp > this.to) {
						this.to = measure.timestamp;
				}
				if(!this.from || measure.timestamp < this.from) {
						this.from = measure.timestamp;
				}
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
