class SensorNode {
    constructor(p = {}) {
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
        this.sensor_systems = [];

        if (p.sensor_system) {
            this.sensor_systems.push(p.sensor_system);
            delete p.sensor_system;
        }

        Object.assign(this, p);
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
    //console.log(this.sensor_systems);
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
    this.merged = [];
    Object.assign(this, p);
  }

  different(obj) {
    const keys = [
      'parent_sensor_id',
      'sensor_id',
      'version_id',
      'life_cycle_id',
      'readme',
    ];
    return keys.some( key => {
      let value = this[key];
      return typeof(value)!=='object' && value != obj[key];
    });
  }

  merge(obj) {
    if(obj.sensor_id != this.sensor_id) {
      console.warn('You are trying to merge non-matching versions');
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

  json() {
    return stripNulls({
      parent_sensor_id: this.parent_sensor_id,
      version_id: this.version_id,
      sensor_id: this.sensor_id,
      life_cycle_id: this.life_cycle_id,
      filename: this.filename,
      merged: this.merged,
      readme: this.readme,
    });
  }
}

class Sensor {
    constructor(p = {}) {
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

        Object.assign(this, p);
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
