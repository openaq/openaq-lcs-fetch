
const {
  getObject,
  putObject
} = require('./utils');


class SensorNode {
  constructor(p = {}) {
    this.map = {
      city: "sensor_node_city",
      country: "sensor_node_country",
      project: "sensor_node_project",
      height_meters: "sensor_node_height_meters",
      distance_from_road_meters: "sensor_node_distance_from_road_meters",
      location_type: "sensor_node_location_type",
      ismobile: "sensor_node_ismobile",
      is_mobile: "sensor_node_ismobile",
      mobile: "sensor_node_ismobile",
    };
    this.sensor_node_id = null;
    this.sensor_node_site_name = null;
    this.sensor_node_source_name = null;
    this.sensor_node_site_description = null;
    this.sensor_node_deployed_by = null;
    this.sensor_node_deployed_date = null;
    this.sensor_node_deploy_notes = null;
    this.sensor_node_ismobile = null;
    this.sensor_node_geometry = null;
    this.sensor_node_timezone = null;
    this.sensor_node_reporting_frequency = null;
    this.sensor_node_city = null;
    this.sensor_node_country = null;
    this.sensor_node_project = null,
    this.sensor_node_location_type = null;
    this.sensor_node_height_meters = null;
    this.sensor_node_distance_from_road_meters = null;

    this.sensor_systems = [];

    if (p.sensor_system) {
      this.sensor_systems.push(p.sensor_system);
      delete p.sensor_system;
    }

    for(const [key, value] of Object.entries(p)) {
      // update the key using the map but default to the provided key
      const k = this.map[key] || key;
      if(k) this[k] = value;
    }
    console.debug('Created new sensor node', this.sensor_node_id);
  }

  addSensor(obj) {
    let ss;
    if(!this.sensor_systems.length) {
      ss = new SensorSystem();
      this.sensor_systems.push(ss);
    } else {
      ss = this.sensor_systems[0];
    }
    ss.addSensor(obj);
  }

  merge(obj) {
    const ignore = ['sensor_systems','sensor_node_geometry'];
    for(const [key, value] of Object.entries(this)) {
      if(!ignore.includes(key)
         && !!obj[key]
         && obj[key] != value) {
        this[key] = obj[key];
      }
    }
    if(obj.sensor_node_geometry) {
      this.sensor_node_geometry = obj.sensor_node_geometry;
    }
    // If both nodes have only one system we can attempt to merge
    if(obj.sensor_systems
       && obj.sensor_systems.length === 1
       && this.sensor_systems.length === 1) {
      const existing = this.sensor_systems[0].sensors.map(d => d.sensor_id);
      const added = [];
      obj.sensor_systems[0].sensors.map( s => {
        if(!existing.includes(s.sensor_id)) {
          this.sensor_systems[0].sensors.push(new Sensor(s));
          added.push(s.sensor_id);
        }
      });
    }
  }


  key() {
    const stack = process.env.STACK;
    const provider = process.env.PROVIDER;
    return `${stack}/stations/${provider}/${this.sensor_node_id}.json.gz`;
  }

  async get() {
    const key = this.key();
    if(!this.stored) {
      this.stored = await getObject(key);
    }
    return this.stored;
  }

  async put() {
    const key = this.key();
    //console.debug('PUTTING SENSOR NODE', key);
    const current = this.get();
    // make sure its different first
    // if(this.different()) {
    await putObject(this.json(), key);
    this.stored = false;
    return true;
    // }
  }


  json() {
    return stripNulls({
      sensor_node_id: this.sensor_node_id,
      sensor_node_site_name: this.sensor_node_site_name,
      sensor_node_source_name: this.sensor_node_source_name,
      sensor_node_site_description: this.sensor_node_site_description,
      sensor_node_deployed_by: this.sensor_node_deployed_by,
      sensor_node_deployed_date: this.sensor_node_deployed_date,
      sensor_node_deploy_notes: this.sensor_node_deploy_notes,
      sensor_node_ismobile: this.sensor_node_ismobile,
      sensor_node_geometry: this.sensor_node_geometry,
      sensor_node_timezone: this.sensor_node_timezone,
      sensor_node_reporting_frequency: this.sensor_node_reporting_frequency,
      sensor_node_city: this.sensor_node_city,
      sensor_node_country: this.sensor_node_country,
      sensor_node_project: this.sensor_node_project,
      sensor_node_location_type: this.sensor_node_location_type,
      sensor_node_height_meters: this.sensor_node_height_meters,
      sensor_node_distance_from_road_meters: this.sensor_node_distance_from_road_meters,
      sensor_systems: this.sensor_systems.map((s) => s.json())
    });
  }
}

class SensorSystem {
  constructor(p = {}) {
    this.sensor_system_id = null;
    this.sensor_system_metadata_effective_tsa = null;
    this.sensor_system_cost_band = null;
    this.sensor_system_description = null;
    this.sensor_system_deployed_by = null;
    this.sensor_system_deployment_date = null;
    this.sensor_system_deployment_notes = null;
    this.sensor_system_firmware_version = null;
    this.sensor_system_height_from_ground_meter = null;
    this.sensor_system_inlet_orientation = null;
    this.sensor_system_manufacturer_batch_number = null;
    this.sensor_system_manufacturer_name = null;
    this.sensor_system_model_name = null;
    this.sensor_system_model_version_name = null;
    this.sensor_system_origin_date = null;
    this.sensor_system_purchase_date = null;
    this.sensor_system_serial_number = null;
    this.sensor_system_source_id = null;
    this.sensor_system_attribution = null;

    this.sensors = [];

    if (p.sensor) {
      this.sensors.push(p.sensor);
      delete p.sensor;
    }

    Object.assign(this, p);
  }

  addSensor(obj) {
    this.sensors.push(obj);
  }

  json() {
    return stripNulls({
      sensor_system_id: this.sensor_system_id,
      sensor_node_id: this.sensor_node_id,
      sensor_system_metadata_effective_tsa: this.sensor_system_metadata_effective_tsa,
      sensor_system_cost_band: this.sensor_system_cost_band,
      sensor_system_description: this.sensor_system_description,
      sensor_system_deployed_by: this.sensor_system_deployed_by,
      sensor_system_deployment_date: this.sensor_system_deployment_date,
      sensor_system_deployment_notes: this.sensor_system_deployment_notes,
      sensor_system_firmware_version: this.sensor_system_firmware_version,
      sensor_system_height_from_ground_meter: this.sensor_system_height_from_ground_meter,
      sensor_system_inlet_orientation: this.sensor_system_inlet_orientation,
      sensor_system_manufacturer_batch_number: this.sensor_system_manufacturer_batch_number,
      sensor_system_manufacturer_name: this.sensor_system_manufacturer_name,
      sensor_system_model_name: this.sensor_system_model_name,
      sensor_system_model_version_name: this.sensor_system_model_version_name,
      sensor_system_origin_date: this.sensor_system_origin_date,
      sensor_system_purchase_date: this.sensor_system_purchase_date,
      sensor_system_serial_number: this.sensor_system_serial_number,
      sensor_system_source_id: this.sensor_system_source_id,
      sensor_system_attribution: this.sensor_system_attribution,
      sensors: this.sensors.map((s) => s.json())
    });
  }
}

class Version {
  constructor(p = {}) {
	  this.parent_sensor_id = null;
	  this.version_id = null;
	  this.sensor_id = null;
	  this.life_cycle_id = null;
	  this.readme = null;
	  this.filename = null;
	  this.parameter = null;
	  this.merged = [];
    this.provider = null;
    this.stored = null;
	  Object.assign(this, p);
    //console.debug('Created new version', this.sensor_id);
  }

  different(obj) {
    const keys = [
      'parent_sensor_id',
      'sensor_id',
      'version_id',
	    'life_cycle_id',
	    'parameter',
      'readme',
    ];
    return keys.some( key => {
      let value = this[key];
      return typeof(value)!=='object' && value != obj[key];
    });
  }

  merge(obj) {
    if(obj.sensor_id != this.sensor_id) {
      console.warn(`You are trying to merge non-matching versions, ${obj.sensor_id} to ${this.sensor_id}`);
      return;
    }
    if(obj.merged && obj.merged.length) {
      this.merged = [ ...this.merged, ...obj.merged ];
    }
    if(obj.readme) {
      this.readme = obj.readme;
      this.merged.push(obj.filename);
    }
  }

  key() {
    const stack = process.env.STACK;
    const provider = process.env.PROVIDER;
    return `${stack}/versions/${provider}/${this.sensor_id}.json.gz`;
  }

  async get() {
    const key = this.key();
    if(!this.stored) {
      this.stored = await getObject(key);
    }
    return this.stored;
  }

  async put() {
    const key = this.key();
    //console.debug('PUTTING VERSION', key);
    const current = this.get();
    // make sure its different first
    // if(this.different()) {
    return await putObject(this.json(), key);
    // }
  }

  json() {
    return stripNulls({
      parent_sensor_id: this.parent_sensor_id,
      version_id: this.version_id,
	    sensor_id: this.sensor_id,
	    parameter: this.parameter,
      life_cycle_id: this.life_cycle_id,
      filename: this.filename,
      // merged: this.merged,
      readme: this.readme,
    });
  }
}

class Sensor {
  constructor(p = {}) {
    this.map = {
      manufacturer_name: 'sensor_manufacturer_name',
      model_name: 'sensor_model_name',
      interval_seconds: 'sensor_data_averaging_period',
      calibration_date: 'sensor_calibration_date',
      last_calibration_timestamp: 'sensor_last_calibration_timestamp',
      last_service_timestamp: 'sensor_last_service_timestamp',
      calibration_procedure: 'sensor_calibration_procedure',
      deployment_date: 'sensor_deployment_date',
      service_date: 'sensor_service_date',
      flow_rate: 'sensor_flow_rate',
      firmware_version: 'sensor_firmware_version',
      size_range: 'sensor_size_range',
    };
    this.sensor_id = null;
    this.sensor_system_id = null;
    this.sensor_data_averaging_period = null;
    this.sensor_data_averaging_period_unit = null;
    this.sensor_data_logging_interval_second = null;
    this.sensor_lifecycle_stage = null;
    this.sensor_description = null;
    this.sensor_deployed_by = null;
    this.sensor_deployment_date = null;
    this.sensor_deactivation_date = null;
    this.sensor_deployment_notes = null;
    this.sensor_firmware_version = null;
    this.sensor_flow_rate_unit = null;
    this.sensor_flow_rate = null;
    this.sensor_calibration_procedure = null;
    this.sensor_last_calibration_timestamp = null;
    this.sensor_last_service_timestamp = null;
    this.sensor_manufacturer_batch_number = null;
    this.sensor_manufacturer_name = null;
    this.sensor_model_name = null;
    this.sensor_model_version_name = null;
    this.sensor_origin_date = null;
    this.sensor_purchase_date = null;
    this.sensor_sampling_duration = null;
    this.sensor_serial_number = null;
    this.measurand_parameter = null;
    this.measurand_unit = null;

    for(const [key, value] of Object.entries(p)) {
      // update the key using the map but default to the provided key
      const k = this.map[key] || key;
      if(k) this[k] = value;
    }
    //console.debug('Created new sensor', this.sensor_id);
  }


  json() {
    return stripNulls({
      sensor_id: this.sensor_id,
      sensor_system_id: this.sensor_system_id,
      sensor_data_averaging_period: this.sensor_data_averaging_period,
      sensor_data_averaging_period_unit: this.sensor_data_averaging_period_unit,
      sensor_data_logging_interval_second: this.sensor_data_logging_interval_second,
      sensor_lifecycle_stage: this.sensor_lifecycle_stage,
      sensor_description: this.sensor_description,
      sensor_deployed_by: this.sensor_deployed_by,
      sensor_deployment_date: this.sensor_deployment_date,
      sensor_deactivation_date: this.sensor_deactivation_date,
      sensor_deployment_notes: this.sensor_deployment_notes,
      sensor_firmware_version: this.sensor_firmware_version,
      sensor_flow_rate_unit: this.sensor_flow_rate_unit,
      sensor_flow_rate: this.sensor_flow_rate,
      sensor_calibration_procedure: this.sensor_calibration_procedure,
      sensor_last_calibration_timestamp: this.sensor_last_calibration_timestamp,
      sensor_last_service_timestamp: this.sensor_last_service_timestamp,
      sensor_manufacturer_batch_number: this.sensor_manufacturer_batch_number,
      sensor_manufacturer_name: this.sensor_manufacturer_name,
      sensor_model_name: this.sensor_model_name,
      sensor_model_version_name: this.sensor_model_version_name,
      sensor_origin_date: this.sensor_origin_date,
      sensor_purchase_date: this.sensor_purchase_date,
      sensor_sampling_duration: this.sensor_sampling_duration,
      sensor_serial_number: this.sensor_serial_number,
      measurand_parameter: this.measurand_parameter,
      measurand_unit: this.measurand_unit
    });
  }
}

function stripNulls(obj) {
  return Object.assign(
    {},
    ...Object.entries(obj)
    // eslint-disable-next-line no-unused-vars
      .filter(([_, v]) => v !== null)
      .map(([k, v]) => ({ [k]: v }))
  );
}

module.exports = {
  Sensor,
  Version,
  SensorNode,
  SensorSystem
};
